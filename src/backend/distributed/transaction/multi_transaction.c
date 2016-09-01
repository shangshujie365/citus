/*-------------------------------------------------------------------------
 *
 * multi_transaction.c
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif

#include "libpq-fe.h"

#include "miscadmin.h"

#include "access/hash.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commit_protocol.h" /* for MultiShardCommitProtocol FIXME */
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_transaction.h"
#include "distributed/resource_lock.h"
#include "mb/pg_wchar.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


#define CLIENT_CONNECT_TIMEOUT_SECONDS "5"

/* GUC, determining whether statements sent to remote nodes are logged */
bool LogRemoteCommands = false;

/* state needed to keep track of operations used during a transaction */
XactModificationType XactModificationLevel = XACT_MODIFICATION_NONE;

typedef enum CoordinatedTransactionState
{
	COORDINATED_TRANSACTION_NONE,
	/* FIXME: introduce state for "implicit commit" style xacts? */
	COORDINATED_TRANSACTION_IDLE,
	COORDINATED_TRANSACTION_STARTED,
	COORDINATED_TRANSACTION_PREPARED,
	COORDINATED_TRANSACTION_COMMITTED
} CoordinatedTransactionState;

CoordinatedTransactionState CurrentCoordinatedTransactionState;

/*
 * three hashes:
 * 1) (host, port, user, database) -> [connections]
 * 2) (placementid) -> [connections]
 * 3) (shardid) -> [ConnectionPlacementEntry]
 */

/* 1) */
typedef struct ConnectionHashKey
{
	char hostname[MAX_NODE_LENGTH];
	int32 port;
	char user[NAMEDATALEN];
	char database[NAMEDATALEN];
} ConnectionHashKey;

typedef struct ConnectionHashEntry
{
	ConnectionHashKey key;
	List *connections;
} ConnectionHashEntry;
static HTAB *ConnectionHash = NULL;


/* 2) */
typedef struct ConnectionPlacementHashKey
{
	uint32 placementid;
} ConnectionPlacementHashKey;
typedef struct ConnectionPlacementHashEntry
{
	ConnectionPlacementHashKey key;
	bool failed;
	bool required;
	List *connections;
} ConnectionPlacementHashEntry;

static HTAB *ConnectionPlacementHash = NULL;


/* 3) */
typedef struct ConnectionShardHashKey
{
	uint32 shardId;
} ConnectionShardHashKey;
typedef struct ConnectionShardHashEntry
{
	ConnectionShardHashKey *key;
	List *placementConnections;
} ConnectionShardHashEntry;

static HTAB *ConnectionShardHash = NULL;


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static bool subXactAbortAttempted = false;

static void CoordinatedTransactionCallback(XactEvent event, void *arg);
static void CoordinatedSubTransactionCallback(SubXactEvent event, SubTransactionId subId,
											  SubTransactionId parentSubid, void *arg);

/* functions for the various hashes */
static inline uint32 hash_combine(uint32 a, uint32 b);
static void hash_delete_all(HTAB *htab);
static uint32 ConnectionHashHash(const void *key, Size keysize);
static int ConnectionHashCompare(const void *a, const void *b, Size keysize);

/* libpq support functions */
static bool IsResponseOK(PGresult *result);
static void ForgetResults(MultiConnection *connection);
static int SendRemoteCommand(MultiConnection *connection, const char *command);
static void LogRemoteCommand(MultiConnection *connection, const char *command);
static void FinishConnectionEstablishment(MultiConnection *connection);
static MultiConnection * StartConnectionEstablishment(ConnectionHashKey *key);


static void AssociatePlacementShard(ConnectionPlacementHashEntry *placementEntry, ShardPlacement *placement);


/* -------------------------------------------------------------------------
 * Coordinated transaction management functions.
 * -------------------------------------------------------------------------
 */


extern void
BeginCoordinatedTransaction(void)
{
	if (CurrentCoordinatedTransactionState != COORDINATED_TRANSACTION_NONE)
	{
		ereport(ERROR, (errmsg("starting transaction in wrong state")));
	}

	CurrentCoordinatedTransactionState = COORDINATED_TRANSACTION_STARTED;
}


extern void
BeginOrContinueCoordinatedTransaction(void)
{
	if (CurrentCoordinatedTransactionState == COORDINATED_TRANSACTION_STARTED)
	{
		return;
	}

	BeginCoordinatedTransaction();
}


extern bool
InCoordinatedTransaction(void)
{
	return CurrentCoordinatedTransactionState != COORDINATED_TRANSACTION_NONE;
}


static void
Assign2PCIdentifier(MultiConnection *connection)
{
	static uint64 sequence = 0;
	snprintf(connection->prepared_name, NAMEDATALEN, "citus_%d_"UINT64_FORMAT,
			 MyProcPid, sequence++);
}


static void
CheckForFailedPlacements(bool preCommit)
{
	HASH_SEQ_STATUS status;
	ConnectionShardHashEntry *shardEntry = NULL;

	hash_seq_init(&status, ConnectionShardHash);
	while ((shardEntry = (ConnectionShardHashEntry *) hash_seq_search(&status)) != 0)
	{
		ListCell *placementCell = NULL;
		int failures = 0;
		int successes = 0;

		foreach(placementCell, shardEntry->placementConnections)
		{
			ConnectionPlacementHashEntry *placementEntry =
				(ConnectionPlacementHashEntry *) lfirst(placementCell);

			ListCell *connectionCell = NULL;

			foreach (connectionCell, placementEntry->connections)
			{
				MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

				if (connection->transactionState & MULTI_TRANSACTION_STATE_FAILED)
				{
					Assert(!(connection->transactionState & MULTI_TRANSACTION_STATE_COMMITTED));
					placementEntry->failed = true;
				}
			}

			if (placementEntry->failed)
			{
				/* FIXME: see fixme above the below ERROR */
				if (placementEntry->required)
				{
					ereport(ERROR, (errmsg("required placement failed")));
				}
				failures++;
			}
			else
			{
				successes++;
			}
		}

		if (failures > 0 && successes == 0)
		{
			/*
			 * FIXME: arguably we should only error out here if we're
			 * pre-commit or using 2PC. Otherwise we can end up with a state
			 * where parts of hte transaction is committed and others aren't,
			 * without correspondingly marking things as invalid.
			 */
			/* FIXME: better message */
			ereport(ERROR, (errmsg("could not commit transaction on any active nodes")));
		}

		foreach(placementCell, shardEntry->placementConnections)
		{
			ConnectionPlacementHashEntry *placementEntry =
				(ConnectionPlacementHashEntry *) lfirst(placementCell);

			if (placementEntry->failed)
			{
				UpdateShardPlacementState(placementEntry->key.placementid, FILE_INACTIVE);
			}
		}
	}
}


static void
CoordinatedTransactionsCommit(void)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;
	ListCell *connectionCell;

	/*
	 * Issue appropriate transaction commands to remote nodes. If everything
	 * went well that's going to be COMMIT or COMMIT PREPARED, if individual
	 * connections had errors, some or all of them might require a ROLLBACK.
	 */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		foreach(connectionCell, entry->connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

			/* nothing to do if no transaction active */
			if (!(connection->transactionState & MULTI_TRANSACTION_STATE_STARTED))
			{
				continue;
			}

			if (connection->transactionState & MULTI_TRANSACTION_STATE_FAILED)
			{
				/*
				 * Try sending an ROLLBACK; Depending on the state that won't
				 * have success, but let's try.  Have to clear previous
				 * results first.
				 */
				ForgetResults(connection); /* try to clear pending stuff */
				if (!SendRemoteCommand(connection, "ROLLBACK;"))
				{
					/* no point in reporting a likely redundant message */
					connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
				}
				else
				{
					connection->transactionState |= MULTI_TRANSACTION_STATE_ABORTING;
				}
			}
			else if (connection->transactionState & MULTI_TRANSACTION_STATE_PREPARED)
			{
				StringInfoData command;

				initStringInfo(&command);
				appendStringInfo(&command, "COMMIT PREPARED '%s';", connection->prepared_name);

				connection->transactionState |= MULTI_TRANSACTION_STATE_COMMITTING;

				if (!SendRemoteCommand(connection, command.data))
				{
					ReportConnectionError(connection, WARNING);
					connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
				}
			}
			else
			{
				connection->transactionState |= MULTI_TRANSACTION_STATE_COMMITTING;

				if (!SendRemoteCommand(connection, "COMMIT;"))
				{
					/* for a moment there I thought we were in trouble */
					ReportConnectionError(connection, WARNING);
					connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
				}
			}
		}
	}

	/* Wait for result */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		foreach(connectionCell, entry->connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
			PGresult *result = NULL;

			if (!(connection->transactionState & (MULTI_TRANSACTION_STATE_COMMITTING |
												  MULTI_TRANSACTION_STATE_ABORTING)))
			{
				continue;
			}

			result = PQgetResult(connection->conn);

			if (!IsResponseOK(result))
			{
				ReportResultError(connection, result, WARNING);
				connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
			}
			else if (connection->transactionState & MULTI_TRANSACTION_STATE_ABORTING)
			{
				connection->transactionState &= ~MULTI_TRANSACTION_STATE_ABORTING;
				connection->transactionState |= MULTI_TRANSACTION_STATE_ABORTED;
			}
			else
			{
				connection->transactionState &= ~MULTI_TRANSACTION_STATE_COMMITTING;
				connection->transactionState |= MULTI_TRANSACTION_STATE_COMMITTED;
			}

			PQclear(result);

			ForgetResults(connection);
		}
	}
}


static void
CoordinatedTransactionsAbort(void)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;
	ListCell *connectionCell;

	/* issue ROLLBACK; to all relevant remote nodes */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		foreach(connectionCell, entry->connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

			if (!(connection->transactionState & MULTI_TRANSACTION_STATE_STARTED))
			{
				continue;
			}

			if (connection->transactionState & MULTI_TRANSACTION_STATE_PREPARED)
			{
				StringInfoData command;

				initStringInfo(&command);
				appendStringInfo(&command, "ROLLBACK PREPARED '%s';", connection->prepared_name);

				if (!SendRemoteCommand(connection, command.data))
				{
					ReportConnectionError(connection, WARNING);
					connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
				}
				else
				{
					connection->transactionState |= MULTI_TRANSACTION_STATE_ABORTING;
				}
			}
			else
			{
				/*
				 * Try sending an ROLLBACK; Depending on the state
				 * that won't have success, but let's try.  Have
				 * to clear previous results first.
				 */
				ForgetResults(connection);
				if (!SendRemoteCommand(connection, "ROLLBACK;"))
				{
					/* no point in reporting a likely redundant message */
					connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
				}
				else
				{
					connection->transactionState |= MULTI_TRANSACTION_STATE_ABORTING;
				}
			}
		}
	}

	/* Wait for result */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		foreach(connectionCell, entry->connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
			PGresult *result = NULL;

			if (!(connection->transactionState & MULTI_TRANSACTION_STATE_ABORTING))
			{
				continue;
			}

			result = PQgetResult(connection->conn);

			if (!IsResponseOK(result))
			{
				/* FIXME: skip reporting this? */
				ReportResultError(connection, result, WARNING);
				connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
			}
			else
			{
				connection->transactionState = MULTI_TRANSACTION_STATE_INVALID;
			}

			result = PQgetResult(connection->conn);
			Assert(!result);
		}
	}
}


static void
CoordinatedTransactionsPrepare(void)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;
	ListCell *connectionCell;

	/* issue PREPARE TRANSACTION; to all relevant remote nodes */

	/* TODO: skip connections that haven't done any DML/DDL */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		foreach(connectionCell, entry->connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

			if (!(connection->transactionState & MULTI_TRANSACTION_STATE_STARTED))
			{
				continue;
			}

			if (!(connection->transactionState & MULTI_TRANSACTION_STATE_FAILED))
			{
				StringInfoData command;

				initStringInfo(&command);

				Assign2PCIdentifier(connection);

				appendStringInfo(&command, "PREPARE TRANSACTION '%s'",
								 connection->prepared_name);


				if (!SendRemoteCommand(connection, command.data))
				{
					/* for a moment there I thought we were in trouble */
					ReportConnectionError(connection, WARNING);
					connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
				}
				else
				{
					connection->transactionState |= MULTI_TRANSACTION_STATE_PREPARING;
				}

			}
		}
	}

	/* Wait for result */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		foreach(connectionCell, entry->connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
			PGresult *result = NULL;

			if (!(connection->transactionState & MULTI_TRANSACTION_STATE_PREPARING))
			{
				continue;
			}

			result = PQgetResult(connection->conn);

			if (!IsResponseOK(result))
			{
				ReportResultError(connection, result, WARNING);
				connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
			}
			else
			{
				connection->transactionState &= ~MULTI_TRANSACTION_STATE_PREPARING;
				connection->transactionState |= MULTI_TRANSACTION_STATE_PREPARED;
			}

			result = PQgetResult(connection->conn);
			Assert(!result);
		}
	}

	CurrentCoordinatedTransactionState = COORDINATED_TRANSACTION_PREPARED;
}


/*
 * Perform connection management activity after the end of a transaction. Both
 * COMMIT and ABORT paths are handled here.
 */
static void
HandleTransactionEnd(void)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;

	/*
	 * Close all remote connections if necessary anymore (i.e. not session
	 * lifetime), or if in a failed state.
	 */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		ListCell *previousCell = NULL;
		ListCell *nextCell = NULL;
		ListCell *connectionCell = NULL;

		for (connectionCell = list_head(entry->connections);
			 connectionCell != NULL;
			 connectionCell = nextCell)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

			nextCell = lnext(connectionCell);

			if (!connection->sessionLifespan ||
				PQstatus(connection->conn) != CONNECTION_OK ||
				PQtransactionStatus(connection->conn) != PQTRANS_IDLE)
			{
				PQfinish(connection->conn);
				connection->conn = NULL;

				entry->connections =
					list_delete_cell(entry->connections, connectionCell, previousCell);

				pfree(connection);
			}
			else
			{
				connection->activeInXact = false;
				connection->transactionState = 0;
				MarkConnectionIdle(connection);

				previousCell = connectionCell;
			}

		}

		/*
		 * NB: We leave the hash entry in place, even if there's no individual
		 * connections in it anymore. There seems no benefit in deleting it,
		 * and it'll save a bit of work in the next transaction.
		 */
	}

	/*
	 * Reset shard/placement associations.
	 */
	hash_delete_all(ConnectionPlacementHash);
	hash_delete_all(ConnectionShardHash);
}


static void
CoordinatedTransactionCallback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
			if (CurrentCoordinatedTransactionState == COORDINATED_TRANSACTION_PREPARED)
			{
				/* handles both already prepared and open transactions */
				CoordinatedTransactionsCommit();
			}

			/* close connections etc. */
			if (CurrentCoordinatedTransactionState != COORDINATED_TRANSACTION_NONE)
			{
				HandleTransactionEnd();
			}

			Assert(!subXactAbortAttempted);
			CurrentCoordinatedTransactionState = COORDINATED_TRANSACTION_NONE;
			XactModificationLevel = XACT_MODIFICATION_NONE;
			break;
		case XACT_EVENT_ABORT:
			/*
			 * FIXME: Add warning for the COORDINATED_TRANSACTION_COMMITTED
			 * case. That can be reached if this backend fails after the
			 * XACT_EVENT_PRE_COMMIT state.
			 */

			/* handles both already prepared and open transactions */
			if (CurrentCoordinatedTransactionState > COORDINATED_TRANSACTION_IDLE)
			{
				CoordinatedTransactionsAbort();
			}

			if (CurrentCoordinatedTransactionState != COORDINATED_TRANSACTION_NONE)
			{
				/* close connections etc. */
				HandleTransactionEnd();
			}

			CurrentCoordinatedTransactionState = COORDINATED_TRANSACTION_NONE;
			XactModificationLevel = XACT_MODIFICATION_NONE;
			subXactAbortAttempted = false;
			break;

		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
			break;
		case XACT_EVENT_PRE_COMMIT:
			/* nothing further to do if there's no managed remote xacts */
			if (CurrentCoordinatedTransactionState == COORDINATED_TRANSACTION_NONE)
			{
				break;
			}

			if (subXactAbortAttempted)
			{
				subXactAbortAttempted = false;

				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot ROLLBACK TO SAVEPOINT in transactions "
									   "which modify distributed tables")));
			}

			/*
			 * Check whether the coordinated transaction is in a state we want
			 * to persist, or whether we want to error out.  This handles the
			 * case that iteratively executed commands marked all placements
			 * as invalid.
			 */
			CheckForFailedPlacements(true);

			/*
			 * TODO: It's probably a good idea to force constraints and such
			 * to 'immediate' here. Deferred triggers might try to send stuff
			 * to the remote side, which'd not be good.
			 */

			if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
			{
				CoordinatedTransactionsPrepare();
				CurrentCoordinatedTransactionState = COORDINATED_TRANSACTION_PREPARED;
			}
			else
			{
				/*
				 * Have to commit remote transactions in PRE_COMMIT, to allow
				 * us to mark failed placements as invalid.  Better don't use
				 * this for anything important (i.e. DDL/metadata).
				 */
				CoordinatedTransactionsCommit();
				CurrentCoordinatedTransactionState = COORDINATED_TRANSACTION_COMMITTED;
			}

			/*
			 * Check again whether shards/placement successfully
			 * committed. This handles failure at COMMIT/PREPARE time.
			 */
			CheckForFailedPlacements(false);

			break;
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			/* FIXME: do we want to support this? Or error out? */
			break;
	}
}


static void
CoordinatedSubTransactionCallback(SubXactEvent event, SubTransactionId subId,
							 SubTransactionId parentSubid, void *arg)
{
	/* FIXME: warn about subxacts outside of a coordinated xact? */
	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		subXactAbortAttempted = true;
	}
}


/* -------------------------------------------------------------------------
 * Connection management functions
 * -------------------------------------------------------------------------
 */


extern MultiConnection *
GetNodeConnection(uint32 flags, const char *hostname, int32 port)
{
	return GetNodeUserDatabaseConnection(flags, hostname, port, NULL, NULL);
}


extern MultiConnection *
StartNodeConnection(uint32 flags, const char *hostname, int32 port)
{
	return StartNodeUserDatabaseConnection(flags, hostname, port, NULL, NULL);
}


static void
FinishConnectionEstablishment(MultiConnection *connection)
{
	while (true)
	{
		ConnStatusType status = PQstatus(connection->conn);
		PostgresPollingStatusType pollmode;

		if (status == CONNECTION_OK)
		{
			return;
		}

		/* FIXME: retries? */
		if (status == CONNECTION_BAD)
		{
			return;
		}

		pollmode = PQconnectPoll(connection->conn);

		/* FIXME: retries? */
		if (pollmode == PGRES_POLLING_FAILED)
		{
			return;
		}
		else if (pollmode == PGRES_POLLING_OK)
		{
			return;
		}
		else if (pollmode == PGRES_POLLING_WRITING ||
				 pollmode == PGRES_POLLING_READING)
		{
		}
		else
		{
			Assert(false);
		}

		while (true)
		{
			struct pollfd pollFileDescriptor;
			int pollResult = 0;

			pollFileDescriptor.fd = PQsocket(connection->conn);
			if (pollmode == PGRES_POLLING_READING)
				pollFileDescriptor.events = POLLIN;
			else
				pollFileDescriptor.events = POLLOUT;
			pollFileDescriptor.revents = 0;

			pollResult = poll(&pollFileDescriptor, 1, 5 /* XXX */);

			if (pollResult >= 0)
			{
				break;
			}
			else if (pollResult != EINTR)
			{
				/* retrying, signal */
			}
			else
			{
				ereport(ERROR, (errcode_for_socket_access(),
								errmsg("poll() failed: %m")));
			}
		}
	}
}


extern MultiConnection *
GetNodeUserDatabaseConnection(uint32 flags, const char *hostname, int32 port, const char *user, const char *database)
{
	MultiConnection *connection;

	connection = StartNodeUserDatabaseConnection(flags, hostname, port, user, database);

	FinishConnectionEstablishment(connection);

	return connection;
}


static MultiConnection *
StartConnectionEstablishment(ConnectionHashKey *key)
{
	char nodePortString[12];
	const char *clientEncoding = GetDatabaseEncodingName();
	MultiConnection *connection = NULL;

	const char *keywords[] = {
		"host", "port", "fallback_application_name",
		"client_encoding", "connect_timeout", "dbname", "user", NULL
	};
	const char *values[] = {
		key->hostname, nodePortString, "citus", clientEncoding,
		CLIENT_CONNECT_TIMEOUT_SECONDS, key->database, key->user, NULL
	};

	connection = MemoryContextAllocZero(CacheMemoryContext, sizeof(MultiConnection));
	sprintf(nodePortString, "%d", key->port);

	strlcpy(connection->hostname, key->hostname, MAX_NODE_LENGTH);
	connection->port = key->port;
	strlcpy(connection->database, key->database, NAMEDATALEN);
	strlcpy(connection->user, key->user, NAMEDATALEN);

	connection->conn = PQconnectStartParams(keywords, values, false);
	return connection;
}


extern MultiConnection *
StartNodeUserDatabaseConnection(uint32 flags, const char *hostname, int32 port, const char *user, const char *database)
{
	ConnectionHashKey key;
	ConnectionHashEntry *entry = NULL;
	MultiConnection *connection;
	MemoryContext oldContext;
	bool found;

	strlcpy(key.hostname, hostname, MAX_NODE_LENGTH);
	key.port = port;
	if (user)
	{
		strlcpy(key.user, user, NAMEDATALEN);
	}
	else
	{
		strlcpy(key.user, CurrentUserName(), NAMEDATALEN);

	}
	if (database)
	{
		strlcpy(key.database, database, NAMEDATALEN);
	}
	else
	{
		strlcpy(key.database, get_database_name(MyDatabaseId), NAMEDATALEN);
	}

	if (CurrentCoordinatedTransactionState == COORDINATED_TRANSACTION_NONE)
	{
		CurrentCoordinatedTransactionState = COORDINATED_TRANSACTION_IDLE;
	}

	/*
	 * Lookup relevant hash entry. We always enter. If only a cached
	 * connection is desired, and there's none, we'll simply leave the
	 * connection list empty.
	 */

	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->connections = NIL;
	}

	if (flags & CACHED_CONNECTION)
	{
		ListCell *connectionCell = NULL;

		/* check connection cache for a connection that's not already in use */
		foreach(connectionCell, entry->connections)
		{
			connection = (MultiConnection *) lfirst(connectionCell);

			if (!connection->busy)
			{
				if ((flags & SESSION_LIFESPAN) && !connection->sessionLifespan)
				{
					connection->sessionLifespan = true;
				}
				connection->activeInXact = true;

				/*
				 * Check whether we're right now allowed to open new
				 * connections. A cached connection counts as new if it hasn't
				 * been used in this transaction.
				 *
				 * FIXME: This likely can be removed soon.
				 */
				if (!connection->activeInXact && XactModificationLevel > XACT_MODIFICATION_DATA)
				{
					ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
									errmsg("cannot open new connections after the first modification "
										   "command within a transaction")));
				}

				return connection;
			}

			/* XXX: do we want to error out if a connection is in failed state? */
		}

		/* no connection available, done if a new connection isn't desirable */
		if (!(flags & NEW_CONNECTION))
		{
			return NULL;
		}
	}

	/*
	 * Check whether we're right now allowed to open new connections.
	 *
	 * FIXME: This likely can be removed soon.
	 */
	if (XactModificationLevel > XACT_MODIFICATION_DATA)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("cannot open new connections after the first modification "
							   "command within a transaction")));
	}

	/*
	 * Either no caching desired, or no connection present. Start connection
	 * establishment.
	 */
	oldContext = MemoryContextSwitchTo(CacheMemoryContext);

	connection = StartConnectionEstablishment(&key);

	entry->connections = lappend(entry->connections, connection);

	MemoryContextSwitchTo(oldContext);

	if (flags & SESSION_LIFESPAN)
	{
		connection->sessionLifespan = true;
	}
	connection->activeInXact = true;

	return connection;
}


void
MarkConnectionBusy(MultiConnection *connection)
{
	Assert(!connection->busy);
	connection->busy = true;
}


void
MarkConnectionIdle(MultiConnection *connection)
{
	connection->busy = false;
}


void
MarkPlacementConnectionFailed(ShardPlacement *placement)
{
	ConnectionPlacementHashKey placementKey;
	ConnectionPlacementHashEntry *placementEntry = NULL;
	bool found = false;

	/* There seems to be little point in making this work */
	Assert(InCoordinatedTransaction());

	placementKey.placementid = placement->placementId;

	placementEntry = hash_search(ConnectionPlacementHash, &placementKey, HASH_ENTER, &found);
	if (!found)
	{
		placementEntry->connections = NIL;
		placementEntry->required = false;
	}

	placementEntry->failed = true;

	AssociatePlacementShard(placementEntry, placement);
}


void
MarkPlacementConnectionRequired(ShardPlacement *placement)
{
	ConnectionPlacementHashKey placementKey;
	ConnectionPlacementHashEntry *placementEntry = NULL;
	bool found = false;

	/* There seems to be little point in making this work */
	Assert(InCoordinatedTransaction());

	placementKey.placementid = placement->placementId;

	placementEntry = hash_search(ConnectionPlacementHash, &placementKey, HASH_ENTER, &found);
	if (!found)
	{
		placementEntry->connections = NIL;
		placementEntry->failed = false;
	}

	placementEntry->required = true;

	AssociatePlacementShard(placementEntry, placement);
}


static void
AssociatePlacementShard(ConnectionPlacementHashEntry *placementEntry, ShardPlacement *placement)
{
	ConnectionShardHashKey shardKey;
	ConnectionShardHashEntry *shardEntry = NULL;
	bool found = false;
	MemoryContext oldContext = NULL;

	shardKey.shardId = placement->shardId;
	shardEntry = hash_search(ConnectionShardHash, &shardKey, HASH_ENTER, &found);
	if (!found)
	{
		shardEntry->placementConnections = NIL;
	}

	/* XXX: has to be unique, right? */
	oldContext = MemoryContextSwitchTo(CacheMemoryContext);
	shardEntry->placementConnections =
		list_append_unique_ptr(shardEntry->placementConnections, placementEntry);
	MemoryContextSwitchTo(oldContext);
}


extern MultiConnection *
StartPlacementConnection(uint32 flags, ShardPlacement *placement)
{
	ConnectionPlacementHashKey key;
	ConnectionPlacementHashEntry *placementEntry = NULL;
	MultiConnection *connection;
	MemoryContext oldContext;
	bool found;

	key.placementid = placement->placementId;

	/* FIXME: not implemented */
	Assert(flags & NEW_CONNECTION);

	/*
	 * Lookup relevant hash entry. We always enter. If only a cached
	 * connection is desired, and there's none, we'll simply leave the
	 * connection list empty.
	 */

	placementEntry = hash_search(ConnectionPlacementHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		placementEntry->connections = NIL;
		placementEntry->failed = false;
		placementEntry->required = false;
	}

	/* XXX: do we want to error out if placement is in failed state? */

	if (flags & CACHED_CONNECTION)
	{
		ListCell *connectionCell = NULL;

		/* check connection cache for a connection that's not already in use */
		foreach(connectionCell, placementEntry->connections)
		{
			connection = (MultiConnection *) lfirst(connectionCell);

			if (!connection->busy)
			{
				return connection;
			}

			/* XXX: do we want to error out if a connection is in failed state? */
		}

		/* no connection available, done if a new connection isn't desirable */
		if (!(flags & NEW_CONNECTION))
		{
			Assert(false); /* unclear what htat means */
			return NULL;
		}
	}

	/*
	 * Either no caching desired, or no connection present. Start connection
	 * establishment.
	 */
	oldContext = MemoryContextSwitchTo(CacheMemoryContext);

	/* FIXME: user / database handling? */
	connection = StartNodeConnection(flags, placement->nodeName, placement->nodePort);

	placementEntry->connections = lappend(placementEntry->connections, connection);

	AssociatePlacementShard(placementEntry, placement);

	MemoryContextSwitchTo(oldContext);

	return connection;
}


extern MultiConnection *
GetPlacementConnection(uint32 flags, ShardPlacement *placement)
{
	MultiConnection *connection = StartPlacementConnection(flags, placement);

	FinishConnectionEstablishment(connection);
	return connection;
}


static void
AdjustRemoteTransactionStates(List *connectionList)
{
	ListCell *connectionCell = NULL;

	if (!InCoordinatedTransaction())
		return;
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

		Assert(PQtransactionStatus(connection->conn) != PQTRANS_ACTIVE);

		if ((connection->transactionState & MULTI_TRANSACTION_STATE_FAILED) ||
			(connection->transactionState & MULTI_TRANSACTION_STATE_STARTING) ||
			(connection->transactionState & MULTI_TRANSACTION_STATE_STARTED))
		{
			continue;
		}

		if (PQtransactionStatus(connection->conn) != PQTRANS_INTRANS)
		{
			/*
			 * Check whether we're right now allowed to start new client
			 * transaction.  FIXME: This likely can be removed soon.
			 */
			if (XactModificationLevel > XACT_MODIFICATION_NONE)
			{
				ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
								errmsg("cannot open new connections after the first modification "
									   "command within a transaction")));
			}

			/* FIXME: specify isolation level etc. */
			if (!SendRemoteCommand(connection, "BEGIN;"))
			{
				ReportConnectionError(connection, WARNING);
				connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
			}
			else
			{
				connection->transactionState |= MULTI_TRANSACTION_STATE_STARTING;
			}
		}
	}


	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		PGresult *result = NULL;

		if (connection->transactionState & MULTI_TRANSACTION_STATE_FAILED)
		{
			continue;
		}

		if (!(connection->transactionState & MULTI_TRANSACTION_STATE_STARTING))
		{
			continue;
		}

		result = PQgetResult(connection->conn);
		if (!IsResponseOK(result))
		{
			connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
			connection->transactionState &= ~MULTI_TRANSACTION_STATE_STARTING;
			ReportResultError(connection, result, WARNING);
		}
		else
		{
			connection->transactionState &= ~MULTI_TRANSACTION_STATE_STARTING;
			connection->transactionState |= MULTI_TRANSACTION_STATE_STARTED;
		}

		PQclear(result);

		ForgetResults(connection);

		Assert(PQtransactionStatus(connection->conn) == PQTRANS_INTRANS);
	}
}


void
AdjustRemoteTransactionState(MultiConnection *connection)
{
	if (InCoordinatedTransaction())
	{
		List *connectionList = list_make1(connection);

		AdjustRemoteTransactionStates(connectionList);
		list_free(connectionList);
	}
}

/* -------------------------------------------------------------------------
 * Execution related functions.
 * -------------------------------------------------------------------------
 */

/*
 * Execute commands on placements in parallel.
 *
 * TODO: Use less than one one connection per placement.
 */
List *
ExecuteCommandOnPlacements(List *shardPlacementList, List *commandList)
{
	List *connectionList = NIL;
	ListCell *placementCell = NULL;
	ListCell *commandCell = NULL;
	ListCell *connectionCell = NULL;
	List *resultList = NIL;

	Assert(list_length(shardPlacementList) == list_length(commandList));

	/*
	 * Initiate connection establishment if necessary. All connections might
	 * be already existing and, possibly, fully established.
	 */
	foreach(placementCell, shardPlacementList)
	{
		ShardPlacement *placement = NULL;
		MultiConnection *connection = NULL;

		Assert(CitusIsA(lfirst(placementCell), ShardPlacement));
		placement = (ShardPlacement *) lfirst(placementCell);

		/* asynchronously open connection to remote node */
		connection =
			StartPlacementConnection(NEW_CONNECTION | CACHED_CONNECTION, placement);

		/* couldn't work with that */
		Assert(PQtransactionStatus(connection->conn) != PQTRANS_ACTIVE);

		/* every command should get its own connection for now */
		MarkConnectionBusy(connection);

		connectionList = lappend(connectionList, connection);
	}

	/* wait for connection establishment */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

		/*
		 * It'd better to wait for all connections at once. Especially when
		 * SSL (or complex authentication protocols), it's quite beneficial to
		 * do connection establishment fully in parallel using nonblocking
		 * IO. This way we'll currently do the initial connect() in parallel,
		 * but afterwards block in SSL connection establishment, which often
		 * takes the bulk of the time.
		 */
		FinishConnectionEstablishment(connection);
	}

	/* BEGIN transaction if necessary */
	AdjustRemoteTransactionStates(connectionList);

	/* Finally send commands to all connections in parallel */
	forboth(commandCell, commandList,
			connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		const char *command = lfirst(commandCell);

		if (connection->transactionState & MULTI_TRANSACTION_STATE_FAILED)
		{
			continue;
		}

		if (!SendRemoteCommand(connection, command))
		{
			ReportConnectionError(connection, WARNING);
			connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
		}
	}

	/*
	 * Wait for command results to come in.
	 *
	 * TODO: We should really wait asynchronously, using nonblocking IO, on
	 * all these connections. As long as they all only tranfer miniscule
	 * amounts of data, it doesn't matter much, but as soon that that's not
	 * the case...
	 */
	forboth(connectionCell, connectionList,
			placementCell, shardPlacementList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		PGresult *result = NULL;
		CommandResult *commandResult = palloc0(sizeof(CommandResult));

		if (connection->transactionState & MULTI_TRANSACTION_STATE_FAILED)
		{
			commandResult->failed = true;
			resultList = lappend(resultList, commandResult);
			continue;
		}

		result = PQgetResult(connection->conn);

		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, WARNING);
			connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
			commandResult->failed = true;

			MarkPlacementConnectionFailed(placement);
		}
		else
		{
			char *affectedTuples = PQcmdTuples(result);
			if (strlen(affectedTuples) > 0)
				scanint8(affectedTuples, false, &commandResult->tuples);

			commandResult->failed = false;
		}

		/* XXX: allow for result processing? */
		PQclear(result);

		/* clear NULL result(s) */
		ForgetResults(connection);

		resultList = lappend(resultList, commandResult);

		/* allow connection to be used again */
		MarkConnectionIdle(connection);
	}

	return resultList;
}


int64
ExecuteQueryOnShards(Query *query, List *shardIntervalList, Oid relationId)
{
	List *commandList = NIL;
	List *placementList = NIL;
	ListCell *intervalCell = NULL;
	List *commandResults = NIL;
	ListCell *commandResultCell = NULL;
	ListCell *placementCell = NULL;
	int64 ntuples = 0;
	int64 lastSuccessfulShardId = INVALID_SHARD_ID;

	foreach(intervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(intervalCell);
		List *shardPlacementList = FinalizedShardPlacementList(shardInterval->shardId);
		ListCell *placementCell = NULL;
		StringInfoData shardQueryString;

		initStringInfo(&shardQueryString);

		deparse_shard_query(query, relationId, shardInterval->shardId, &shardQueryString);

		foreach(placementCell, shardPlacementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);

			placementList = lappend(placementList, placement);
			commandList = lappend(commandList, shardQueryString.data);
		}
	}

	commandResults = ExecuteCommandOnPlacements(placementList, commandList);

	forboth(commandResultCell, commandResults, placementCell, placementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		CommandResult *commandResult = (CommandResult *) lfirst(commandResultCell);

		if (!commandResult->failed)
		{
			if (lastSuccessfulShardId != placement->shardId)
			{
				ntuples += commandResult->tuples;
			}
			lastSuccessfulShardId = placement->shardId;
		}
	}

	return ntuples;
}


void
ExecuteDDLOnRelationPlacements(Oid relationId, const char *command)
{
	/* FIXME: locking???? */
	List *shardIntervalList = LoadShardIntervalList(relationId);
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);
	char *escapedCommandString = quote_literal_cstr(command);
	StringInfo applyCommand = makeStringInfo();
	List *commandList = NIL;
	List *execPlacementList = NIL;
	ListCell *intervalCell = NULL;
	List *resultList = NIL;

	BeginOrContinueCoordinatedTransaction();

	LockShards(shardIntervalList, ShareLock);

	foreach(intervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(intervalCell);
		List *placementList = FinalizedShardPlacementList(shardInterval->shardId);
		uint64 shardId = shardInterval->shardId;
		ListCell *placementCell = NULL;

		/* build the shard ddl command -- perhaps add parametrized variant instead? */
		appendStringInfo(applyCommand, WORKER_APPLY_SHARD_DDL_COMMAND, shardId,
						 escapedSchemaName, escapedCommandString);

		foreach (placementCell, placementList)
		{
			execPlacementList = lappend(execPlacementList, lfirst(placementCell));
			commandList = lappend(commandList, pstrdup(applyCommand->data));
		}

		resetStringInfo(applyCommand);
	}

	resultList = ExecuteCommandOnPlacements(execPlacementList, commandList);

	/* for DDL we cannot tolerate any failures */
	RequireCommandSuccess(execPlacementList, resultList);
}


void
RequireCommandSuccess(List *placementList, List *commandResults)
{
	ListCell *commandResultCell = NULL;
	ListCell *placementCell = NULL;

	forboth(commandResultCell, commandResults, placementCell, placementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		CommandResult *commandResult = (CommandResult *) lfirst(commandResultCell);

		if (commandResult->failed)
		{
			/* FIXME: polish */
			ereport(ERROR, (errmsg("could not execute DDL command on worker node shards")));
		}
		/* signal that later failure on connection would be fatal */
		MarkPlacementConnectionRequired(placement);
	}
}


static void
HandlePlacementFailure(List *goodPlacements, List *failedPlacements)
{
	if (list_length(failedPlacements) > 0 &&
		list_length(goodPlacements) == 0)
	{
		elog(ERROR, "all placements failed");
	}
	else if (list_length(failedPlacements) > 0)
	{
		elog(LOG, "some placements failed, marking as invalid");
	}
}


void
InvalidateFailedPlacements(List *placementList, List *commandResults)
{
	ListCell *commandResultCell = NULL;
	ListCell *placementCell = NULL;
	int64 lastShardId = INVALID_SHARD_ID;
	List *failedPlacements = NIL;
	List *goodPlacements = NIL;

	forboth(commandResultCell, commandResults, placementCell, placementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		CommandResult *commandResult = (CommandResult *) lfirst(commandResultCell);

		if (lastShardId != INVALID_SHARD_ID && lastShardId != placement->shardId)
		{
			HandlePlacementFailure(goodPlacements, failedPlacements);
			failedPlacements = NIL;
			goodPlacements = NIL;
		}

		if (commandResult->failed)
		{
			failedPlacements = lappend(failedPlacements, placement);
		}
		else
		{
			goodPlacements = lappend(goodPlacements, placement);
		}
	}

	HandlePlacementFailure(goodPlacements, failedPlacements);
}


PGresult *
ExecuteStatement(MultiConnection *connection, const char *statement)
{
	return ExecuteStatementParams(connection, statement, 0, NULL, NULL);
}

PGresult *
ExecuteStatementParams(MultiConnection *connection, const char *statement,
					   int paramCount, const Oid *paramTypes,
					   const char *const *paramValues)
{
	PGresult *result = NULL;

	AdjustRemoteTransactionState(connection);

	if (connection->transactionState & MULTI_TRANSACTION_STATE_FAILED)
	{
		return NULL;
	}

	LogRemoteCommand(connection, statement);
	if (!PQsendQueryParams(connection->conn, statement, paramCount, paramTypes, paramValues,
						   NULL, NULL, 0))
	{
		ReportConnectionError(connection, WARNING);
		connection->transactionState |= MULTI_TRANSACTION_STATE_FAILED;
		return NULL;
	}

	result = PQgetResult(connection->conn);

	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, WARNING);
		PQclear(result);
		result = PQgetResult(connection->conn);
		Assert(result == NULL);

		return NULL;
	}

	return result;
}

bool
ExecuteCheckStatement(MultiConnection *connection, const char *statement)
{
	return ExecuteCheckStatementParams(connection, statement, 0, NULL, NULL);
}

bool
ExecuteCheckStatementParams(MultiConnection *connection, const char *statement,
							int paramCount, const Oid *paramTypes,
							const char *const *paramValues)
{
	bool resultOk = false;
	PGresult *result = ExecuteStatementParams(connection, statement, paramCount, paramTypes,
											  paramValues);

	resultOk = result != NULL;
	PQclear(result);

	result = PQgetResult(connection->conn);
	Assert(result == NULL);

	return resultOk;
}


/* -------------------------------------------------------------------------
 * Hash related functions.
 * -------------------------------------------------------------------------
 */


/*
 * Combine two hash values, resulting in another hash value, with decent bit
 * mixing.
 *
 * Similar to boost's hash_combine().
 */
static inline uint32
hash_combine(uint32 a, uint32 b)
{
	a ^= b + 0x9e3779b9 + (a << 6) + (a >> 2);
	return a;
}


static void
hash_delete_all(HTAB *htab)
{
	HASH_SEQ_STATUS status;
	void *entry = NULL;

	hash_seq_init(&status, htab);
	while ((entry = hash_seq_search(&status)) != 0)
	{
		bool found = false;

		hash_search(htab, entry, HASH_REMOVE, &found);
		Assert(found);
	}
}

static uint32
ConnectionHashHash(const void *key, Size keysize)
{
	ConnectionHashKey *entry = (ConnectionHashKey *) key;
	uint32 hash = 0;

	hash = string_hash(entry->hostname, NAMEDATALEN);
	hash = hash_combine(hash, hash_uint32(entry->port));
	hash = hash_combine(hash, string_hash(entry->user, NAMEDATALEN));
	hash = hash_combine(hash, string_hash(entry->database, NAMEDATALEN));

	return hash;
}


static int
ConnectionHashCompare(const void *a, const void *b, Size keysize)
{
	ConnectionHashKey *ca = (ConnectionHashKey *) a;
	ConnectionHashKey *cb = (ConnectionHashKey *) b;

	if (strncmp(ca->hostname, cb->hostname, NAMEDATALEN) != 0 ||
		ca->port != cb->port ||
		strncmp(ca->user, cb->user, NAMEDATALEN) != 0 ||
		strncmp(ca->database, cb->database, NAMEDATALEN) != 0)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

/* libpq support functions */

/*
 * IsResponseOK checks the resultStatus and returns true if the status is OK.
 */
static bool
IsResponseOK(PGresult *result)
{
	ExecStatusType resultStatus = PQresultStatus(result);

	if (resultStatus == PGRES_SINGLE_TUPLE || resultStatus == PGRES_TUPLES_OK ||
		resultStatus == PGRES_COMMAND_OK)
	{
		return true;
	}

	return false;
}

/*
 * Tiny PQsendQuery wrapper that logs remote commands, and accepts a
 * MultiConnection instead of a plain PGconn.
 */
static int
SendRemoteCommand(MultiConnection *connection, const char *command)
{
	LogRemoteCommand(connection, command);
	return PQsendQuery(connection->conn, command);
}


/*
 * Clear connection from current activity.
 *
 * FIXME: This probably should use PQcancel() if results would require network
 * IO.
 */
static void
ForgetResults(MultiConnection *connection)
{
	while (true)
	{
		PGresult *result = NULL;
		result = PQgetResult(connection->conn);
		if (result == NULL)
			break;
		if (PQresultStatus(result) == PGRES_COPY_IN)
		{
			PQputCopyEnd(connection->conn, NULL);
			/* FIXME: mark connection as failed? */
		}
		PQclear(result);
	}
}

void
ReportConnectionError(MultiConnection *connection, int elevel)
{
	char *nodeName = connection->hostname;
	int nodePort = connection->port;

	ereport(elevel, (errmsg("connection error: %s:%d", nodeName, nodePort),
					  errdetail("%s", PQerrorMessage(connection->conn))));
}


void
ReportResultError(MultiConnection *connection, PGresult *result, int elevel)
{
	char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
	char *messagePrimary = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
	char *messageDetail = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);
	char *messageHint = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT);
	char *messageContext = PQresultErrorField(result, PG_DIAG_CONTEXT);

	char *nodeName = connection->hostname;
	int nodePort = connection->port;
	int sqlState = ERRCODE_CONNECTION_FAILURE;

	if (sqlStateString != NULL)
	{
		sqlState = MAKE_SQLSTATE(sqlStateString[0], sqlStateString[1], sqlStateString[2],
								 sqlStateString[3], sqlStateString[4]);
	}

	/*
	 * If the PGresult did not contain a message, the connection may provide a
	 * suitable top level one. At worst, this is an empty string.
	 */
	if (messagePrimary == NULL)
	{
		char *lastNewlineIndex = NULL;

		messagePrimary = PQerrorMessage(connection->conn);
		lastNewlineIndex = strrchr(messagePrimary, '\n');

		/* trim trailing newline, if any */
		if (lastNewlineIndex != NULL)
		{
			*lastNewlineIndex = '\0';
		}
	}

	if (sqlState == ERRCODE_CONNECTION_FAILURE)
	{
		ereport(elevel, (errcode(sqlState),
						 errmsg("connection failed to %s:%d", nodeName, nodePort),
						 errdetail("%s", messagePrimary)));
	}
	else
	{
		ereport(elevel, (errcode(sqlState), errmsg("%s", messagePrimary),
						 messageDetail ? errdetail("%s", messageDetail) : 0,
						 messageHint ? errhint("%s", messageHint) : 0,
						 messageContext ? errcontext("%s", messageContext) : 0,
						 errcontext("while executing command on %s:%d",
									nodeName, nodePort)));
	}
}


static void
LogRemoteCommand(MultiConnection *connection, const char *command)
{
	if (!LogRemoteCommands)
	{
		return;
	}

	/* FIXME: Add host etc. */
	ereport(LOG, (errmsg("issuing %s", command)));
}


static void
InitializeTransactionManagement(void)
{
	HASHCTL info;
	uint32 hashFlags = 0;

	/* hook into transaction machinery */
	RegisterXactCallback(CoordinatedTransactionCallback, NULL);
	RegisterSubXactCallback(CoordinatedSubTransactionCallback, NULL);

	/*
	 * XXX: Do we want a memory context for this module's allocation? That'd
	 * make analysis of memory usage a bit easier than having some stuff
	 * (besides the hash contexts, which are recognizable) directly under
	 * CacheMemoryContext.
	 */

	/* create (host,port,user,database) -> [connection] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ConnectionHashKey);
	info.entrysize = sizeof(ConnectionHashEntry);
	info.hash = ConnectionHashHash;
	info.match = ConnectionHashCompare;
	info.hcxt = CacheMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);

	ConnectionHash = hash_create("citus connection cache (host,port,user,database)",
								 64, &info, hashFlags);


	/* create (placementid) -> [connection] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ConnectionPlacementHashKey);
	info.entrysize = sizeof(ConnectionPlacementHashEntry);
	info.hash = tag_hash;
	info.hcxt = CacheMemoryContext;
	hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	ConnectionPlacementHash = hash_create("citus connection cache (placementid)",
										  64, &info, hashFlags);

	/* create (shardId) -> [ConnectionShardHashEntry] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ConnectionShardHashKey);
	info.entrysize = sizeof(ConnectionShardHashEntry);
	info.hash = tag_hash;
	info.hcxt = CacheMemoryContext;
	hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	ConnectionShardHash = hash_create("citus connection cache (shardid)",
									  64, &info, hashFlags);

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
}

void
InstallTransactionManagementShmemHook(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = InitializeTransactionManagement;
}
