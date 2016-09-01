/*-------------------------------------------------------------------------
 *
 * multi_transaction.h
 *	  Type and function declarations used in performing transactions across
 *	  the cluster.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_TRANSACTION_H
#define MULTI_TRANSACTION_H

#include "distributed/master_metadata_utility.h"

/* maximum (textual) lengths of hostname and port */
#define MAX_NODE_LENGTH 255 /* includes 0 byte */

/* forward declare, to avoid forcing libpq on everyone */
struct pg_conn; /* target of the PGconn typedef */
struct pg_result; /* target of the PGresult typedef */

/* Enumeration that defines different remote transaction states */
typedef enum
{
	/* no transaction active */
	MULTI_TRANSACTION_STATE_INVALID = 0,

	/* error occurred on connection */
	MULTI_TRANSACTION_STATE_FAILED = 1 << 0,

	/* transaction start */
	MULTI_TRANSACTION_STATE_STARTING = 1 << 1,
	MULTI_TRANSACTION_STATE_STARTED = 1 << 2,

	/* 2pc prepare */
	MULTI_TRANSACTION_STATE_PREPARING = 1 << 4,
	MULTI_TRANSACTION_STATE_PREPARED = 1 << 5,

	/* transaction abort */
	MULTI_TRANSACTION_STATE_ABORTING = 1 << 6,
	MULTI_TRANSACTION_STATE_ABORTED = 1 << 7,

	/* transaction commit */
	MULTI_TRANSACTION_STATE_COMMITTING = 1 << 8,
	MULTI_TRANSACTION_STATE_COMMITTED = 1 << 9,

	MULTI_TRANSACTION_STATE_DDL = 1 << 10,
	MULTI_TRANSACTION_STATE_DML = 1 << 11
} MultiTransactionState;


typedef struct MultiConnection
{
	char hostname[MAX_NODE_LENGTH];
	int32 port;
	char user[NAMEDATALEN];
	char database[NAMEDATALEN];

	char prepared_name[NAMEDATALEN];

	bool sessionLifespan;

	bool busy;

	bool activeInXact;

	struct pg_conn *conn;

	/* move to private part of struct? */
	uint32 transactionState;
} MultiConnection;


typedef enum MultiConnectionMode
{
	NEW_CONNECTION = 1 << 0,
	CACHED_CONNECTION = 1 << 1,
	SESSION_LIFESPAN = 1 << 2
} MultiConnectionMode;


typedef struct CommandResult
{
	bool failed;
	int64 tuples;
} CommandResult;


/* describes what kind of modifications have occurred in the current transaction */
typedef enum
{
	XACT_MODIFICATION_INVALID = 0, /* placeholder initial value */
	XACT_MODIFICATION_NONE,        /* no modifications have taken place */
	XACT_MODIFICATION_DATA,        /* data modifications (DML) have occurred */
	XACT_MODIFICATION_SCHEMA       /* schema modifications (DDL) have occurred */
} XactModificationType;


/* GUC, determining whether statements sent to remote nodes are logged */
extern bool LogRemoteCommands;

/* state needed to prevent new connections during modifying transactions */
extern XactModificationType XactModificationLevel;

/*
 * Initialization.
 */
extern void InstallTransactionManagementShmemHook(void);

/*
 * Low level connection establishment API.
 */
extern MultiConnection * GetNodeConnection(uint32 flags, const char *hostname, int32 port);
extern MultiConnection * StartNodeConnection(uint32 flags, const char *hostname, int32 port);
extern MultiConnection * GetNodeUserDatabaseConnection(uint32 flags, const char *hostname, int32 port, const char *user, const char *database);
extern MultiConnection * StartNodeUserDatabaseConnection(uint32 flags, const char *hostname, int32 port, const char *user, const char *database);

/*
 * Higher level connection handling API.
 */
extern MultiConnection * GetPlacementConnection(uint32 flags, ShardPlacement *placement);
extern MultiConnection * StartPlacementConnection(uint32 flags, ShardPlacement *placement);

/*
 * Coordinated transaction management.
 */
extern void BeginCoordinatedTransaction(void);
extern void BeginOrContinueCoordinatedTransaction(void);
extern bool InCoordinatedTransaction(void);
extern void Force2PCTransaction(void);

/* interactions with a connection */
extern void AdjustRemoteTransactionState(MultiConnection *connection);

extern void MarkConnectionBusy(MultiConnection *connection);
extern void MarkConnectionIdle(MultiConnection *connection);

extern void MarkPlacementConnectionFailed(ShardPlacement *placement);
extern void MarkPlacementConnectionRequired(ShardPlacement *placement);

/* libpq helpers */
extern void ReportConnectionError(MultiConnection *connection, int elevel);
extern void ReportResultError(MultiConnection *connection, struct pg_result *result, int elevel);

extern struct pg_result * ExecuteStatement(MultiConnection *connection, const char *statement);
extern struct pg_result * ExecuteStatementParams(MultiConnection *connection, const char *statement,
												 int paramCount, const Oid *paramTypes,
												 const char *const *paramValues);
extern bool ExecuteCheckStatement(MultiConnection *connection, const char *statement);
extern bool ExecuteCheckStatementParams(MultiConnection *connection, const char *statement,
										int paramCount, const Oid *paramTypes,
										const char *const *paramValues);

/* higher level command execution helpers */
extern List * ExecuteCommandOnPlacements(List *shardPlacementList, List *commandList);
extern int64 ExecuteQueryOnShards(Query *query, List *shardPlacementList, Oid relationId);
extern void ExecuteDDLOnRelationPlacements(Oid relationId, const char *command);

extern void RequireCommandSuccess(List *placementList, List *resultList);
extern void InvalidateFailedPlacements(List *placementList, List *resultList);

#endif /* MULTI_TRANSACTION_H */
