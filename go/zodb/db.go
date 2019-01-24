// Copyright (C) 2018-2019  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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
// application-level database handle.

import (
	"context"
	"sort"
	"sync"
	"time"

	"lab.nexedi.com/kirr/neo/go/transaction"
)

// DB represents a handle to database at application level and contains pool
// of connections. DB.Open opens database connection. The connection will be
// automatically put back into DB pool for future reuse after corresponding
// transaction is complete. DB thus provides service to maintain live objects
// cache and reuse live objects from transaction to transaction.
//
// Note that it is possible to have several DB handles to the same database.
// This might be useful if application accesses distinctly different sets of
// objects in different transactions and knows beforehand which set it will be
// next time. Then, to avoid huge cache misses, it makes sense to keep DB
// handles opened for every possible case of application access.
//
// DB is safe to access from multiple goroutines simultaneously.
type DB struct {
	stor IStorage

	mu    sync.Mutex
	connv []*Connection // order by ↑= .at

	// XXX -> Storage. XXX or -> Cache? (so it is not duplicated many times for many DB case)

	// δtail of database changes for invalidations
	// min(rev) = min(conn.at) for all conn ∈ db (opened and in the pool)
	δtail ΔTail // [](rev↑, []oid)

	// openers waiting for δtail.Head to become covering their at.
	δwait map[δwaiter]struct{} // set{(at, ready)}
}

// δwaiter represents someone waiting for δtail.Head to become ≥ at.
// XXX place
type δwaiter struct {
	at    Tid
	ready chan struct{}
}


// NewDB creates new database handle.
func NewDB(stor IStorage) *DB {
	// XXX db options?
	db := &DB{stor: stor}
	watchq := make(chan CommitEvent)
	stor.AddWatch(watchq)
	// XXX DelWatch? in db.Close() ?
	go db.watcher(watchq)
	return db
}

// ConnOptions describes options to DB.Open .
type ConnOptions struct {
	At     Tid  // if !0, open Connection bound to `at` view of database; not latest.
	NoSync bool // don't sync with storage to get its last tid.
}

// String represents connection options in human-readable form.
//
// For example:
//
//	(@head, sync)
func (opt *ConnOptions) String() string {
	s := "(@"
	if opt.At != 0 {
		s += opt.At.String()
	} else {
		s += "head"
	}

	s += ", "
	if opt.NoSync {
		s += "no"
	}
	s += "sync)"
	return s
}

// watcher receives events about new committed transactions and updates δtail.
//
// it also wakes up δtail waiters.
func (db *DB) watcher(watchq <-chan CommitEvent) { // XXX err ?
	for {
		event, ok := <-watchq
		if !ok {
			// XXX wake up all waiters?
			return // closed
		}

		var readyv []chan struct{} // waiters that become ready

		db.mu.Lock()
		db.δtail.Append(event.Tid, event.Changev)
		for w := range db.δwait {
			if w.at <= event.Tid {
				readyv = append(readyv, w.ready)
				delete(db.δwait, w)
			}
		}
		db.mu.Unlock()

		// wakeup waiters outside of db.mu
		for _, ready := range readyv {
			close(ready)
		}
	}
}

// Open opens new connection to the database.
//
// By default the connection is opened to current latest database state; opt.At
// can be specified to open connection bound to particular view of the database.
//
// Open must be called under transaction.
// Opened connection must be used only under the same transaction and only
// until that transaction is complete.
func (db *DB) Open(ctx context.Context, opt *ConnOptions) (_ *Connection, err error) {
	defer func() {
		if err == nil {
			return
		}

		err = &OpError{
			URL:  db.stor.URL(),
			Op:   "open db",
			Args: opt,
			Err:  err,
		}
	}()

	txn := transaction.Current(ctx)

	at := opt.At
	if at == 0 {
		head := zodb.Tid(0)

		if opt.NoSync {
			// XXX locking
			// XXX prevent retrieved head to be removed from δtail
			head = db.δtail.Head()	// = 0 if empty
		}

		// !NoSync or δtail empty
		// sync storage for lastTid
		if head == 0 {
			var err error

			// XXX stor.LastTid returns last_tid storage itself
			// received on server, not last_tid on server.
			// -> add stor.Sync() ?
			head, err = db.stor.LastTid(ctx)
			if err != nil {
				return nil, err
			}
		}

		at = head
	}

	// wait till .δtail.head is up to date covering ≥ at
	var δready chan struct{}
	db.mu.Lock()
	δhead := δtail.Head()
	// XXX prevent head from going away?
	if δhead < at {
		δready = make(chan struct{})
		db.δwait[δwaiter{at, δready}] = struct{}
	}
	db.mu.Unlock()

	if δready != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-δready:
			// ok
		}
	}

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
	// with minimal distance to get to state @at. If all connections are too
	// distant - create connection anew.
	//
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

	if conn.db != db {
		panic("DB.get: foreign connection in the pool")
	}
	if conn.txn != nil {
		panic("DB.get: live connection in the pool")
	}

	if conn.at != at {
		panic("DB.get: TODO: invalidations")	// XXX
	}

	return conn
}

// put puts connection back into db pool.
func (db *DB) put(conn *Connection) {
	if conn.db != db {
		panic("DB.put: conn.db != db")
	}

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
