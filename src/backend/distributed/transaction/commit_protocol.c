/*-------------------------------------------------------------------------
 *
 * commit_protocol.c
 *     This file contains functions for managing 1PC or 2PC transactions
 *     across many shard placements.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "distributed/commit_protocol.h"
#include "distributed/connection_cache.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/multi_shard_transaction.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

/* the commit protocol to use for COPY commands */
int MultiShardCommitProtocol = COMMIT_PROTOCOL_1PC;
