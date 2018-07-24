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

package transaction

import (
	"context"
	"sync"
)

// transaction implements Transaction.
type transaction struct {
	mu	sync.Mutex
	status	Status
	datav	[]DataManager
	syncv	[]Synchronizer

	// metadata
	user	    string
	description string
	extension   string // XXX
}

// ctxKey is type private to transaction package, used as key in contexts.
type ctxKey struct{}

// getTxn returns transaction associated with provided context.
// nil is returned is there is no association.
func getTxn(ctx context.Context) *transaction {
	t := ctx.Value(ctxKey{})
	if t == nil {
		return nil
	}
	return t.(*transaction)
}

// currentTxn serves Current.
func currentTxn(ctx context.Context) Transaction {
	txn := getTxn(ctx)
	if txn == nil {
		panic("transaction: no current transaction")
	}
	return txn
}

// newTxn serves New.
func newTxn(ctx context.Context) (Transaction, context.Context) {
	if getTxn(ctx) != nil {
		panic("transaction: nested transactions are not supported")
	}

	txn := &transaction{status: Active}
	txnCtx := context.WithValue(ctx, ctxKey{}, txn)
	return txn, txnCtx
}

// Status implements Transaction.
func (txn *transaction) Status() Status {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.status
}

// Commit implements Transaction.
func (txn *transaction) Commit(ctx context.Context) error {
	panic("TODO")
}

// Abort implements Transaction.
func (txn *transaction) Abort() {
	panic("TODO")
}

// Join implements Transaction.
func (txn *transaction) Join(dm DataManager) {
	panic("TODO")
}

// RegisterSync implements Transaction.
func (txn *transaction) RegisterSync(sync Synchronizer) {
	panic("TODO")
}


// ---- meta ----

func (txn *transaction) User() string		{ return txn.user		}
func (txn *transaction) Description() string	{ return txn.description	}
func (txn *transaction) Extension() string	{ return txn.extension		}
