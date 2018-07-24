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

// Package transaction provides transaction management via two-phase commit protocol.
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
// Transactions are represented by Transaction interface. A transaction can be
// started with New, which creates transaction object and remembers it in a
// child of provided context:
//
//	txn, ctx := transaction.New(ctx)
//
// The transaction should be eventually completed by user - either committed or aborted, e.g.
//
//	... // do something with data
//	err := txn.Commit(ctx)
//
// As transactions are associated with contexts, Current returns that associated transaction:
//
//	txn := transaction.Current(ctx)
//
// That might be useful to pass transactions through API boundaries.
//
//
// Contrary to transaction/py there is no relation in between transaction and current thread -
// a transaction scope is managed completely by programmer. In particular it is
// possible to use one transaction in several goroutines simultaneously.
//
// There can be also several in-progress transactions running simultaneously.
//
//
// Two-phase commit
//
// Transactions this package manages, support committing data into several
// backends simultaneously. The way it is organized is via employing two-phase
// commit protocol:
//
//	https://en.wikipedia.org/wiki/Two-phase_commit_protocol
//
// For this scheme to work, every data backend (e.g. database connection) which
// participates in a transaction, must first let the transaction know when the
// data it manages was modified. Then at commit time the transaction manager
// will perform two-phase commit related calls to the backends that joined the
// transaction.
//
// The details of interaction between transaction manager and a backend are in
// DataManager interface. As backends usually provide specific API to users for
// accessing its data, the following example is relevant:
//
//	func (b *MyBackend) ChangeID(ctx context.Context, newID int) {
//		b.id = newID
//
//		// data changed - join the transaction to participate in commit.
//		txn := transaction.Current(ctx)
//		txn.Join(b)
//	}
//
//
// Synchronization
//
// An object, e.g. a backend, might want to be notified of transaction completion events.
// For example
//
//	- backends that do not have hooks installed to every access of its
//	  data, might want to check data dirtiness before transaction
//	  completion starts,
//
//	- backends might need to free some resources after transaction
//	  completes.
//
// Transaction.RegisterSync provides the way to be notified of such
// synchronization points. Please see Synchronizer interface for details.
//
// Notice: transaction/py also provides "new transaction" synchronization point,
// but there is no need for it in transaction/go:
//
//	- the only place where it is used is in zodb/py because data backend
//	  (Connection) is opened while there is no yet transaction started:
//
//	  https://github.com/zopefoundation/ZODB/blob/3.10.7-4-gb8d7a8567/src/ZODB/collaborations.txt#L25-L29
//
//	- the way transaction/go works is to first start a transaction, and
//	  then, under it, open all backends, e.g. ZODB connections. This way
//	  when backend open happens, there is already transaction object
//	  available in the context, and thus there is no need to be notified of
//	  when transaction is created.
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
// XXX nested transactions are not supported.
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
