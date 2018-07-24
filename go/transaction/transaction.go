// Copyright (c) 2001, 2002 Zope Foundation and Contributors.
// All Rights Reserved.
//
// Copyright (C) 2018  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This software is subject to the provisions of the Zope Public License,
// Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
// THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
// WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
// FOR A PARTICULAR PURPOSE.

// Package transaction provides transaction management via Two-Phase commit protocol.
//
// It is modelled after Python transaction package:
//
//	http://transaction.readthedocs.org
//	https://github.com/zopefoundation/transaction
//
// but is not exactly equal to it.
//
//
// Overview
//
// XXX
//
// Transactions are created with New()
package transaction

import (
	"context"
)

// Status describes status of a transaction.
type Status int

const (
	Active Status = iota // transaction is in progress
	Committing           // transaction commit started
	Committed            // transaction commit finished successfully
	// XXX CommitFailed  // transaction commit resulted in error
	// XXX Aborted       // transaction was aborted by user
	// XXX Doomed        // transaction was doomed
)

// Transaction represents a transaction.
//
// ... and should be completed by user via either Commit or Abort.
//
// Before completion, if there are changes to managed data, corresponding
// DataManager(s) must join the transaction to participate in the completion.
type Transaction interface {
	User() string		// user name associated with transaction
	Description() string	// description of transaction

	// XXX map[string]interface{}	(objects must be simple values serialized with pickle or json, not "instances")
	Extension() string



	// Status returns current status of the transaction.
	Status() Status

	// Commit finalizes the transaction.
	//
	// Commit completes the transaction by executing the two-phase commit
	// algorithm for all DataManagers associated with the transaction.
	Commit(ctx context.Context) error

        // Abort aborts the transaction.
	//
	// Abort completes the transaction by executing Abort on all
	// DataManagers associated with it.
	Abort()	// XXX + ctx, error?

	// XXX + Doom?


	// ---- part for data managers & friends ----
	// XXX move to separate interface?

	// Join associates a DataManager to the transaction.
	//
	// Only associated data managers will participate in the transaction
	// completion - commit or abort.
	//
	// Join must be called before transaction completion begins.
	Join(dm DataManager)

	// RegisterSync registers sync to be notified in this transaction boundary events.
	//
	// See Synchronizer for details.
	RegisterSync(sync Synchronizer)

	// XXX SetData(key interface{}, data interface{})
	// XXX GetData(key interface{}) interface{}, ok
}

// New creates new transaction.
//
// XXX the transaction will be associated with ctx (txnCtx derives from ctx + associates txn)
func New(ctx context.Context) (txn Transaction, txnCtx context.Context) {
	return newTxn(ctx)
}

// Current returns current transaction.
//
// It panics if there is no transaction associated with provided context.
func Current(ctx context.Context) Transaction {
	return currentTxn(ctx)
}


// DataManager manages data and can transactionally persist it.
//
// If DataManager is registered to transaction via Transaction.Join, it will
// participate in that transaction completion - commit or abort. In other words
// a data manager have to join to corresponding transaction when it sees there
// are modifications to data it manages.
type DataManager interface {
	// Abort should abort all modifications to managed data.
	//
	// Abort is called by Transaction outside of two-phase commit, and only
	// if abort was caused by user requesting transaction abort. If
	// two-phase commit was started and transaction needs to be aborted due
	// to two-phase commit logic, TPCAbort will be called.
	Abort(txn Transaction)		// XXX +ctx, error

	// TPCBegin should begin commit of a transaction, starting the two-phase commit.
	TPCBegin(txn Transaction)	// XXX +ctx, error ?

        // Commit should commit modifications to managed data.
	//
	// It should save changes to be made persistent if the transaction
	// commits (if TPCFinish is called later). If TPCAbort is called
	// later, changes must not persist.
	//
	// This should include conflict detection and handling. If no conflicts
	// or errors occur, the data manager should be prepared to make the
	// changes persist when TPCFinish is called.
	Commit(ctx context.Context, txn Transaction) error

        // TPCVote should verify that a data manager can commit the transaction.
	//
	// This is the last chance for a data manager to vote 'no'. A data
	// manager votes 'no' by returning an error.
	TPCVote(ctx context.Context, txn Transaction) error

        // TPCFinish should indicate confirmation that the transaction is done.
	//
        // It should make all changes to data modified by this transaction persist.
	//
        // This should never fail. If this returns an error, the	XXX
        // database is not expected to maintain consistency; it's a
        // serious error.
	TPCFinish(ctx context.Context, txn Transaction) error

	// TPCAbort should Abort a transaction.
	//
        // This is called by a transaction manager to end a two-phase commit on
	// the data manager. It should abandon all changes to data modified
	// by this transaction.
	//
        // This should never fail.
	TPCAbort(ctx context.Context, txn Transaction) // XXX error?

	// XXX SortKey() string ?
}

// Synchronizer is the interface to participate in transaction-boundary notifications.
type Synchronizer interface {
	// BeforeCompletion is called before corresponding transaction is going to be completed.
	//
	// The transaction manager calls BeforeCompletion before txn is going
	// to be completed - either committed or aborted.
	BeforeCompletion(ctx context.Context, txn Transaction) error

	// AfterCompletion is called after corresponding transaction was completed.
	//
	// The transaction manager calls AfterCompletion after txn is completed
	// - either committed or aborted.
	AfterCompletion(txn Transaction) // XXX +ctx, error?
}
