// Copyright (C) 2018  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

package zodb
// application-level database connection.

import (
	"context"
	"sort"
	"sync"
	"time"

	"lab.nexedi.com/kirr/neo/go/transaction"
)

// DB represents a handle to database at application level and contains pool
// of connections. If application opens connection via DB.Open, the connection
// will be automatically put back into DB pool for future reuse after
// corresponding transaction is complete. DB thus provides service to maintain
// live objects cache and reuse live objects from transaction to transaction.
//
// Note that it is possible to have several DB handles to the same database.
// This might be useful if application accesses distinctly different sets of
// objects in different transactions and knows beforehand which set it will be
// next time. Then, to avoid huge cache misses, it makes sense to keep DB
// handles opened for every possible case of application access.
//
// DB is safe to access from multiple goroutines simultaneously.
type DB struct {
	stor	IStorage

	mu	sync.Mutex
	connv	[]*Connection // order by ↑= .at

	// information about invalidations
	// XXX -> Storage. XXX or -> Cache? (so it is not duplicated many times for many DB case)
	invTab	[]invEntry // order by ↑= .tid
}

// invEntry describes invalidations caused by a database transaction.
type invEntry struct {
	tid	Tid
	oidv	[]Oid
}


// NewDB creates new database handle.
//
// XXX text, options.
func NewDB(stor IStorage) *DB {
	return &DB{stor: stor}
}

// Open opens new connection to the database.
//
// The connection is opened to current latest database state.
//
// Open must be called under transaction.
// Opened connection must be used under the same transaction only.
//
// XXX text
//
// XXX +OpenAt ?
func (db *DB) Open(ctx context.Context) (*Connection, error) {
	// XXX err ctx

	txn := transaction.Current(ctx)

	// sync storage for lastTid
	// XXX open option not to sync and just get lastTid as .invTab.Head() ?
	at, err := db.stor.LastTid(ctx)
	if err != nil {
		return nil, err
	}

	// wait till .invTab is up to date covering ≥ lastTid
	// XXX reenable
/*
	err = db.invTab.Wait(ctx, at)
	if err != nil {
		return nil, err
	}
*/

	// now we have both at and invalidation data covering it -> proceed to
	// get connection from the pool.
	conn := db.get(at)
	conn.txn = txn
	txn.RegisterSync((*connTxnSync)(conn))

	return conn, nil
}

// get returns connection from db pool most close to at.
//
// it creates new one if there is no close-enough connection in the pool.
func (db *DB) get(at Tid) *Connection {
	db.mu.Lock()
	defer db.mu.Unlock()

	l := len(db.connv)

	// find connv index corresponding to at:
	// [i-1].at ≤ at < [i].at
	i := sort.Search(l, func(i int) bool {
		return at < db.connv[i].at
	})

	// search through window of X previous connections and find out the one
	// with minimal distance to get to state @at. If all connections are to
	// distant - create connection anew.
	// XXX search not only previous, but future too? (we can get back to
	// past by invalidating what was later changed)
	const X = 10 // XXX hardcoded
	jδmin := -1
	for j := i - X; j < i; j++ {
		if j < 0 {
			continue
		}

		// TODO search for max N(live) - N(live, that will need to be invalidated)
		jδmin = j // XXX stub (using rightmost j)
	}

	// nothing found or too distant
	const Tnear = 10*time.Minute // XXX hardcoded
	if jδmin < 0 || tabs(δtid(at, db.connv[jδmin].at)) > Tnear {
		return newConnection(db, at)
	}

	// reuse the connection
	conn := db.connv[jδmin]
	copy(db.connv[jδmin:], db.connv[jδmin+1:])
	db.connv[l-1] = nil
	db.connv = db.connv[:l-1]

	if !(conn.db == db && conn.txn == nil) {
		panic("DB.get: foreign/live connection in the pool")
	}

	if conn.at != at {
		panic("DB.get: TODO: invalidations")
	}

	return conn
}

// put puts connection back into db pool.
func (db *DB) put(conn *Connection) {
	// XXX assert conn.db == db
	conn.txn = nil

	db.mu.Lock()
	defer db.mu.Unlock()

	// XXX check if len(connv) > X, and drop conn if yes
	// [i-1].at ≤ at < [i].at
	i := sort.Search(len(db.connv), func(i int) bool {
		return conn.at < db.connv[i].at
	})

	//db.connv = append(db.connv[:i], conn, db.connv[i:]...)
	db.connv = append(db.connv, nil)
	copy(db.connv[i+1:], db.connv[i:])
	db.connv[i] = conn

	// XXX GC too idle connections here?
}

// ---- txn sync ----

type connTxnSync Connection // hide from public API

func (csync *connTxnSync) BeforeCompletion(txn transaction.Transaction) {
	conn := (*Connection)(csync)
	conn.checkTxn(txn, "BeforeCompletion")
	// nothing
}

// AfterCompletion puts conn back into db pool after transaction is complete.
func (csync *connTxnSync) AfterCompletion(txn transaction.Transaction) {
	conn := (*Connection)(csync)
	conn.checkTxn(txn, "AfterCompletion")

	// XXX check that conn was explicitly closed by user?

	conn.db.put(conn)
}