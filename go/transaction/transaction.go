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
package transaction

import (
	"context"
)

// // Manager manages sequence of transactions.
// type Manager interface {
// 
// 	// Begin begins new transaction.
// 	//
// 	// XXX
// 	//
// 	// XXX There must be no existing in progress transaction.
// 	// XXX kill ^^^ - we should support many simultaneous transactions.
// 	//
// 	// XXX + ctx, error -> yes, i.e. it needs to sync fetch last tid from db
// 	Begin() Transaction
// 
// 	// RegisterSynch registers synchronizer.
// 	//
// 	// Synchronizers are notified about major events in a transaction's life.
// 	// See Synchronizer for details.
// 	RegisterSynch(synch Synchronizer)
// 
// 	// UnregisterSynch unregisters synchronizer.
// 	//
// 	// XXX synchronizer must be already registered?
// 	// XXX or make RegisterSynch return something that will allow to unregister via that?
// 	UnregisterSynch(synch Synchronizer)
// }

// Transaction represents running transaction.
//
// ... and should be completed by either Commit or Abort.
type Transaction interface {
	User() string		// user name associated with transaction
	Description() string	// description of transaction

	// XXX map[string]interface{}	(objects must be simple values serialized with pickle or json, not "instances")
	Extension() string

	// Commit finalizes the transaction.
	//
	// This executes the two-phase commit algorithm for all DataManagers
	// associated with the transaction.
	Commit(ctx context.Context) error

        // Abort aborts the transaction.
	//
        // This is called from the application. This can only be called	XXX
        // before the two-phase commit protocol has been started.	XXX
	Abort()

	// XXX + Doom?

	// Join associates a DataManager to the transaction.
	//
	// Only associated data managers will participate in the transaction commit.
	Join(DataManager)

	// XXX SetData(key interface{}, data interface{})
	// XXX GetData(key interface{}) interface{}, ok
}

// // XXX
// type Participant interface {
// }

// DataManager manages data and can transactionally persist it.
//
// If DataManager is registered to transaction via Transaction.Join, it will
// participate in that transaction commit or abort.
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
        // This includes conflict detection and handling. If no conflicts or
        // errors occur, the committer should be prepared to make the
        // changes persist when TPCFinish is called.
	Commit(ctx context.Context, txn Transaction) error

        // TPCVote should verify that a committer can commit the transaction.
	//
        // This is the last chance for a committer to vote 'no'.  A
        // committer votes 'no' by returning an error.
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
	// BeforeCompletion is called by the transaction at the start of a commit.
	// XXX -> PreCommit ?
	BeforeCompletion(txn Transaction)

	// AfterCompletion is called by the transaction after completing a commit.
	AfterCompletion(txn Transaction)

	// NewTransaction is called at the start of a transaction.
	// XXX -> TxnStart ?
	// XXX + ctx, error (it needs to ask storage for last tid)
	NewTransaction(txn Transaction)
}
