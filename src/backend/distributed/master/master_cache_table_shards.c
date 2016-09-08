/*-------------------------------------------------------------------------
 *
 * master_cache_table_shards.c
 *	  UDF to refresh shard cache at workers
 *
 * This file contains master_cache_table_shards function, it accepts a
 * table name and caches the table's shards at all workers not having
 * shard placement.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"

#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

static void DropShardsFromWorker(WorkerNode *workerNode, char *relationName,
								 List *shardIntervalList);

PG_FUNCTION_INFO_V1(master_expire_table_cache);


/*
 * master_expire_table_cache drops table's caches shards in all workers. The function
 * expects a passed table to be a small distributed table meaning it has less than
 * large_table_shard_count.
 */
Datum
master_expire_table_cache(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *tableName = get_rel_name(relationId);
	char *tableQualifiedName = quote_qualified_identifier(schemaName, tableName);
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);
	List *workerNodeList = NIL;
	ListCell *workerNodeCell = NULL;
	List *workerDroppedShardList = NIL;
	ListCell *workerDroppedShardListCell = NULL;
	int shardCount = 0;
	List **placementListArray = NULL;
	int shardIndex = 0;

	if (!cacheEntry->isDistributedTable)
	{
		ereport(ERROR, (errmsg("Must be called on a distributed table")));
	}

	shardCount = cacheEntry->shardIntervalArrayLength;
	if (shardCount == 0)
	{
		ereport(WARNING, (errmsg("Table has no shards, no action is taken")));
		PG_RETURN_VOID();
	}

	if (shardCount >= LargeTableShardCount)
	{
		ereport(ERROR, (errmsg("Must be called on tables smaller than %d shards",
							   LargeTableShardCount)));
	}

	placementListArray = palloc(shardCount * sizeof(List *));

	for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		ShardInterval *shardInterval = cacheEntry->sortedShardIntervalArray[shardIndex];
		placementListArray[shardIndex] =
			FinalizedShardPlacementList(shardInterval->shardId);
	}

	workerNodeList = WorkerNodeList();

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		List *currentWorkerDropList = NIL;

		for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
		{
			ShardInterval *shardInterval =
				cacheEntry->sortedShardIntervalArray[shardIndex];
			List *placementList = placementListArray[shardIndex];
			ListCell *placementCell = NULL;
			bool found = false;

			foreach(placementCell, placementList)
			{
				ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(placementCell);

				if (shardPlacement->nodePort == workerNode->workerPort &&
					strncmp(shardPlacement->nodeName, workerNode->workerName,
							WORKER_LENGTH) == 0)
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				currentWorkerDropList = lappend(currentWorkerDropList, shardInterval);
			}
		}

		workerDroppedShardList = lappend(workerDroppedShardList, currentWorkerDropList);
	}

	forboth(workerNodeCell, workerNodeList, workerDroppedShardListCell,
			workerDroppedShardList)
	{
		List *shardDropList = (List *) lfirst(workerDroppedShardListCell);
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);

		if (shardDropList == NIL)
		{
			continue;
		}

		DropShardsFromWorker(workerNode, tableQualifiedName, shardDropList);
	}

	pfree(placementListArray);

	PG_RETURN_VOID();
}


static void
DropShardsFromWorker(WorkerNode *workerNode, char *relationName, List *shardIntervalList)
{
	StringInfo workerCommand = makeStringInfo();
	StringInfo tableShardList = makeStringInfo();
	ListCell *shardIntervalCell = NULL;
	bool firstShard = true;
	char storageType = '\0';

	Assert(shardIntervalList != NIL);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);

		if (firstShard)
		{
			firstShard = false;
			storageType = shardInterval->storageType;
		}
		else
		{
			appendStringInfo(tableShardList, ", ");
		}

		appendStringInfo(tableShardList, "%s_" UINT64_FORMAT, relationName,
						 shardInterval->shardId);
	}

	appendStringInfo(tableShardList, " CASCADE");

	if (storageType == SHARD_STORAGE_TABLE)
	{
		appendStringInfo(workerCommand, DROP_REGULAR_TABLE_COMMAND, tableShardList->data);
	}
	else if (storageType == SHARD_STORAGE_COLUMNAR ||
			 storageType == SHARD_STORAGE_FOREIGN)
	{
		appendStringInfo(workerCommand, DROP_FOREIGN_TABLE_COMMAND, tableShardList->data);
	}

	ereport(WARNING, (errmsg("worker command : %s ", workerCommand->data)));

	ExecuteRemoteCommand(workerNode->workerName, workerNode->workerPort, workerCommand);
}
