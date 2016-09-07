/*-------------------------------------------------------------------------
 *
 * commit_protocol.h
 *	  Type and function declarations used in performing transactions across
 *	  shard placements.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMMIT_PROTOCOL_H
#define COMMIT_PROTOCOL_H


#include "access/xact.h"
#include "distributed/multi_transaction.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"


/* Enumeration that defines the different commit protocols available */
typedef enum
{
	COMMIT_PROTOCOL_1PC = 0,
	COMMIT_PROTOCOL_2PC = 1
} CommitProtocolType;

/* Enumeration that defines different remote transaction states */
typedef enum
{
	TRANSACTION_STATE_INVALID = 0,
	TRANSACTION_STATE_OPEN,
	TRANSACTION_STATE_COPY_STARTED,
	TRANSACTION_STATE_PREPARED,
	TRANSACTION_STATE_CLOSED
} TransactionState;

/*
 * TransactionConnection represents a connection to a remote node which is
 * used to perform a transaction on shard placements.
 */
typedef struct TransactionConnection
{
	int64 connectionId;
	/* FIXME: get rid of transactionState, it's duplicated from PGconn */
	TransactionState transactionState;
	MultiConnection *connection;
} TransactionConnection;


/* config variable managed via guc.c */
extern int MultiShardCommitProtocol;

#endif /* COMMIT_PROTOCOL_H */
