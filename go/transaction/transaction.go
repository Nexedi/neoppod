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

// Package transaction provides transaction management via Two-Phase transaction protocol.
//
// It is modelled after Python transaction package:
//
//	http://transaction.readthedocs.org
//	https://github.com/zopefoundation/transaction
package transaction

import (
	"context"
)

// Manager manages seqeunce of transactions.
type Manager interface {

	// Begin begins new transaction.
	//
	// XXX
	//
	// XXX There must be no existing in progress transaction.
	Begin() Transaction

	// RegisterSynch registers synchronizer.
	//
	// Synchronizers are notified about major events in a transaction's life.
	// See Synchronizer for details.
	RegisterSynch(synch Synchronizer)

	// UnregisterSynch unregisters synchronizer.
	//
	// XXX synchronizer must be already registered?
	// XXX or make RegisterSynch return something that will allow to unregister via that?
	UnregisterSynch(synch Synchronizer)
}

// Transaction represents running transaction.
//
// XXX -> concrete type?
type Transaction interface {
	User() string		// user name associated with transaction
	Description() string	// description of transaction
	Extension() string	// XXX

	// Commit finalizes the transaction.
	//
	// This executes the two-phase commit algorithm for all
        // IDataManager objects associated with the transaction.	XXX
	Commit(ctx context.Context) error

        // Abort aborts the transaction.
	//
        // This is called from the application.  This can only be called	XXX
        // before the two-phase commit protocol has been started.		XXX
	Abort()

	// XXX + Doom?

	// Add a data manager to the transaction.				XXX
	Join(dm DataManager)

	// XXX SetData(oid zodb.Oid, data interface{}
	// XXX GetData(oid zodb.Oid) interface{}
}


// DataManager represents transactional storage.	// XXX -> transaction.Storage?
type DataManager interface {
	// TPCBegin should begin commit of a transaction, starting the two-phase commit.
	TPCBegin(txn Transaction)	// XXX +ctx, error ?

        // Commit should commit modifications to registered objects.
	//
        // Save changes to be made persistent if the transaction commits (if
        // tpc_finish is called later).  If tpc_abort is called later, changes
        // must not persist.
	//
        // This includes conflict detection and handling.  If no conflicts or
        // errors occur, the data manager should be prepared to make the
        // changes persist when tpc_finish is called.
	Commit(ctx context.Context, txn Transaction) error

        // TPCVote should verify that a data manager can commit the transaction.
	//
        // This is the last chance for a data manager to vote 'no'.  A
        // data manager votes 'no' by returning an error.
	TPCVote(ctx context.Context, txn Transaction) error

        // Indicate confirmation that the transaction is done.
	//
        // Make all changes to objects modified by this transaction persist.
	//
        // This should never fail.  If this raises an exception, the	XXX
        // database is not expected to maintain consistency; it's a
        // serious error.
	TPCFinish(ctx context.Context, txn Transaction) // XXX error?

	// XXX SortKey() string ?
}


// Synchronizer is the interface to participate in transaction-boundary notifications.
type Synchronizer interface {
	// BeforeCompletion is called by the transaction at the start of a commit.
	BeforeCompletion(txn Transaction)

	// AfterCompletion is called by the transaction after completing a commit.
	AfterCompletion(txn Transaction)

	// NewTransaction is called at the start of a transaction.
	// XXX -> TxnStart ?
	NewTransaction(txn Transaction)
}
