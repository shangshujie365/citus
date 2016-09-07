/*-------------------------------------------------------------------------
 *
 * multi_shard_transaction.h
 *	  Type and function declarations used in performing transactions across
 *	  shard placements.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_SHARD_TRANSACTION_H
#define MULTI_SHARD_TRANSACTION_H


#include "utils/hsearch.h"
#include "nodes/pg_list.h"


/* ShardConnections represents a set of connections for each placement of a shard */
typedef struct ShardConnections
{
	int64 shardId;
	List *connectionList;
} ShardConnections;


extern HTAB * CreateShardConnectionHash(void);
extern ShardConnections * GetShardConnections(HTAB *shardConnectionHash,
											  int64 shardId,
											  bool *shardConnectionsFound);
extern List * ConnectionList(HTAB *connectionHash);
extern void CloseConnections(List *connectionList);


#endif /* MULTI_SHARD_TRANSACTION_H */
