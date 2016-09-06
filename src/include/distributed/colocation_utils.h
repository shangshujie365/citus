/*-------------------------------------------------------------------------
 *
 * colocation_utils.h
 *
 * Declarations for public utility functions related to co-located tables.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLOCATION_UTILS_H_
#define COLOCATION_UTILS_H_

#include "distributed/shardinterval_utils.h"
#include "nodes/pg_list.h"

#define INVALID_COLOCATION_ID 0

extern int TableColocationId(Oid distributedTableId);
extern bool TablesColocated(Oid distributedTableId1, Oid distributedTableId2);
extern bool ShardsColocated(ShardInterval *shard1, ShardInterval *shard2);
//extern List * ColocatedTableList(Oid distributedTableId);
//extern List * ColocatedPlacementList(ShardInterval *shard1, char *sourceHost, char *sourcePort);

#endif /* COLOCATION_UTILS_H_ */
