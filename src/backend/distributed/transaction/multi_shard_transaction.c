/*-------------------------------------------------------------------------
 *
 * multi_shard_transaction.c
 *     This file contains functions for managing 1PC or 2PC transactions
 *     across many shard placements.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "libpq-fe.h"
#include "postgres.h"

#include "distributed/connection_cache.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/multi_shard_transaction.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"


#define INITIAL_CONNECTION_CACHE_SIZE 1001

/*
 * CreateShardConnectionHash constructs a hash table used for shardId->Connection
 * mapping.
 */
HTAB *
CreateShardConnectionHash(void)
{
	HTAB *shardConnectionsHash = NULL;
	int hashFlags = 0;
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(ShardConnections);
	info.hash = tag_hash;
	info.hcxt = TopTransactionContext;

	hashFlags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT;
	shardConnectionsHash = hash_create("Shard Connections Hash",
									   INITIAL_CONNECTION_CACHE_SIZE, &info,
									   hashFlags);

	return shardConnectionsHash;
}


/*
 * GetShardConnections finds existing connections for a shard in the hash.
 * If not found, then a ShardConnections structure with empty connectionList
 * is returned.
 */
ShardConnections *
GetShardConnections(HTAB *shardConnectionHash, int64 shardId,
					bool *shardConnectionsFound)
{
	ShardConnections *shardConnections = NULL;

	shardConnections = (ShardConnections *) hash_search(shardConnectionHash,
														&shardId,
														HASH_ENTER,
														shardConnectionsFound);
	if (!*shardConnectionsFound)
	{
		shardConnections->shardId = shardId;
		shardConnections->connectionList = NIL;
	}

	return shardConnections;
}


/*
 * ConnectionList flattens the connection hash to a list of placement connections.
 */
List *
ConnectionList(HTAB *connectionHash)
{
	List *connectionList = NIL;
	HASH_SEQ_STATUS status;
	ShardConnections *shardConnections = NULL;

	hash_seq_init(&status, connectionHash);

	shardConnections = (ShardConnections *) hash_seq_search(&status);
	while (shardConnections != NULL)
	{
		List *shardConnectionsList = list_copy(shardConnections->connectionList);
		connectionList = list_concat(connectionList, shardConnectionsList);

		shardConnections = (ShardConnections *) hash_seq_search(&status);
	}

	return connectionList;
}
