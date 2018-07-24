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

	"lab.nexedi.com/kirr/go123/xerr"
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
		panic("transaction: new: nested transactions not supported")
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
	ctx := context.Background()	// FIXME stub

	var datav []DataManager
	var syncv []Synchronizer

	// under lock: change state to aborting; extract datav/syncv
	func() {
		txn.mu.Lock()
		defer txn.mu.Unlock()

		txn.checkNotYetCompleting("abort")
		txn.status = Aborting

		datav = txn.datav; txn.datav = nil
		syncv = txn.syncv; txn.syncv = nil
	}()

	// lock released

	// sync.BeforeCompletion -> errBeforeCompletion
	n := len(syncv)
	wg := sync.WaitGroup{}
	wg.Add(n)
	errv := make([]error, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()

			errv[i] = syncv[i].BeforeCompletion(ctx, txn)
		}()
	}
	wg.Wait()

	ev := xerr.Errorv{}
	for _, err := range errv {
		ev.Appendif(err)
	}
	errBeforeCompletion := ev.Err()
	xerr.Context(&errBeforeCompletion, "transaction: abort:")

	// XXX if before completion = err -> skip data.Abort()? state -> AbortFailed?

	// data.Abort
	n = len(datav)
	wg = sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()

			datav[i].Abort(txn)	// XXX err?
		}()
	}
	wg.Wait()

	// XXX set txn status

	// sync.AfterCompletion
	n = len(syncv)
	wg = sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()

			syncv[i].AfterCompletion(txn)
		}()
	}

	// XXX return error?
}


// checkNotYetCompleting asserts that transaction completion has not yet began.
//
// and panics if the assert fails.
// must be called with .mu held.
//
// XXX place
func (txn *transaction) checkNotYetCompleting(who string) {
	switch txn.status {
	case Active:	// XXX + Doomed ?
		// ok
	default:
		panic("transaction: " + who + ": transaction completion already began")
	}
}

// Join implements Transaction.
func (txn *transaction) Join(dm DataManager) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.checkNotYetCompleting("join")

	// XXX forbid double join?
	txn.datav = append(txn.datav, dm)
}

// RegisterSync implements Transaction.
func (txn *transaction) RegisterSync(sync Synchronizer) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.checkNotYetCompleting("register sync")

	// XXX forbid double register?
	txn.syncv = append(txn.syncv, sync)
}


// ---- meta ----

func (txn *transaction) User() string		{ return txn.user		}
func (txn *transaction) Description() string	{ return txn.description	}
func (txn *transaction) Extension() string	{ return txn.extension		}
