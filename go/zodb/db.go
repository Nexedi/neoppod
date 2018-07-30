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
// FOR A PARTICULAR PURPOSE

package zodb
// application-level database connection.

import (
	"context"
	"sync"

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
	connv	[]*Connection
}


// NewDB creates new database handle.
//
// XXX text, options.
func NewDB(stor IStorage) *DB {
	return &DB{stor: stor}
}

// Open opens new connection to the database.
//
// XXX must be called under transaction.
//
// XXX text
func (db *DB) Open(ctx context.Context) *Connection {
	txn := transaction.Current(ctx)

	conn := db.get()

	// XXX sync storage for lastTid -> conn.at

	conn.txn = txn
	txn.RegisterSync((*connTxnSync)(conn))

	return conn
}


// get returns connection from db pool, or creates new one if pool was empty.
func (db *DB) get() *Connection {
	db.mu.Lock()
	defer db.mu.Unlock()

	var conn *Connection
	if l := len(db.connv); l > 0 {
		// pool is !empty - use latest closed conn.
		// XXX zodb/py orders conn ↑ by conn._cache.cache_non_ghost_count.
		// XXX this way top of the stack is the "most live".
		conn = db.connv[l - 1]
		db.connv[l - 1] = nil
		db.connv = db.connv[:l-1]

		// XXX assert conn.txn == nil
	} else {
		conn = &Connection{stor: db.stor, db: db}
	}

	// XXX assert conn.db == db
	return conn
}

// put puts connection back into db pool.
func (db *DB) put(conn *Connection) {
	// XXX assert conn.db == db
	conn.txn = nil
	//XXX want to do this; but also need to preserve info at..last_tid to process invalidations
	//conn.at  = 0

	db.mu.Lock()
	defer db.mu.Unlock()

	// XXX check if len(connv) > X, and drop conn if yes
	db.connv = append(db.connv, conn)
}

// ---- txn sync ----

type connTxnSync Connection // hide from public API

func (conn *connTxnSync) BeforeCompletion(txn transaction.Transaction) {
	// XXX check txn
	// nothing
}

func (conn *connTxnSync) AfterCompletion(txn transaction.Transaction) {
	// XXX check txn

	conn.db.put((*Connection)(conn))
}
