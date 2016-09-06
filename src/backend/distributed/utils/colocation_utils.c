/*-------------------------------------------------------------------------
 *
 * colocation_utils.c
 *
 * This file contains functions to perform useful operations on co-located tables.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"


#include "distributed/colocation_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_planner.h"


/*
 * TableColocationId function returns co-location id of given table
 */
int
TableColocationId(Oid distributedTableId)
{
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);

	return cacheEntry->colocationId;
}


/*
 * TablesColocated function checks whether given two tables are co-located and
 * returns true if they are co-located.
 */
bool
TablesColocated(Oid distributedTableId1, Oid distributedTableId2)
{
	int colocationId1 = TableColocationId(distributedTableId1);
	int colocationId2 = TableColocationId(distributedTableId2);


	if (colocationId1 == INVALID_COLOCATION_ID || colocationId2 == INVALID_COLOCATION_ID)
	{
		return false;
	}

	return colocationId1 == colocationId2;
}


/*
 * ShardsColocated function checks whether given two shards are co-located and
 * returns true if they are co-located.
 */
bool
ShardsColocated(ShardInterval *shard1, ShardInterval *shard2)
{
	int shard1MinValue = DatumGetInt32(shard1->minValue);
	int shard1MaxValue = DatumGetInt32(shard1->maxValue);
	int shard2MinValue = DatumGetInt32(shard2->minValue);
	int shard2MaxValue = DatumGetInt32(shard2->maxValue);

	bool minValuesCheck = shard1MinValue == shard2MinValue;
	bool maxValuesCheck = shard1MaxValue == shard2MaxValue;
	bool tableColocationCheck = TablesColocated(shard1->relationId, shard2->relationId);

	return tableColocationCheck && minValuesCheck && maxValuesCheck;
}

/*
 * ColocatedTableList function return list of distributedTableId which are co-located
 * with given table.

List * ColocatedTableList(Oid distributedTableId)
{
	List *colocatedTables = NIL;

	Oid testedTableId = InvalidOid;
	if (TablesColocated(distributedTableId, testedTableId))
	{
		colocatedTables = lappend(colocatedTables, testedTableId);
	}
}
 */
