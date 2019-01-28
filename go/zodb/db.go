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

	"fmt"
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

	// pool of unused connections.
	//
	// On open(at) live cache is reused through finding conn with nearby
	// .at and invalidating live objects based on δtail info.
	//
	// not all connections here have δtail coverage.
	pool []*Connection // order by ↑= .at

	// δtail of database changes.
	//
	// Used for live cache invalidations on open with at close to current
	// storage head. δtail coverage is maintained based on the following:
	//
	// 1) if open(at) is _far_away_ from head - it is _unlikely_ for
	//    opened connection to be later propagated towards head.
	//
	// 2) if open(at) is _close_      to head - it is _possible_ for
	//    opened connection to be later propagated towards head.
	//
	// For "1" we don't need δtail coverage; for "2" probability that
	// it would make sense for connection to be advanced decreases the
	// longer the connection stays opened. Thus the following 2 factors
	// affect whether it makes sense to keep δtail coverage for a
	// connection:
	//
	//         |at - δhead(when_open)|	ΔTnext		 - avg. time between transactions
	// heady = ──────────────────────	at     		 - connection opened for this state
	//                ΔTnext		δhead(when_open) - δtail.Head when connection was opened
	//					Twork(conn)	 - time the connection is used
	//         Twork(conn)
	// lwork = ───────────
	//           ΔTnext
	//
	// if heady >> 1 - it is case "1" and δtail coverage is not needed.
	// if heady  ~ 1 - it is case "2" and δtail coverage might be needed depending on lwork.
	// if lwork >> 1 - the number of objects that will need to be invalidated
	//		   when updating conn to current head grows to ~ 100% of
	//		   connection's live cache. It thus does not make
	//		   sense to keep δtail past some reasonable time.
	//
	// A good system would monitor both ΔTnext, and lwork for connections
	// with small heady, and adjust δtail cut time as e.g.
	//
	//	timelen(δtail) = 3·lwork·ΔTnext
	//
	//
	// FIXME for now we just fix
	//
	//	Tδkeep = 10min
	//
	// and keep δtail coverage for Tδkeep time
	//
	//	timelen(δtail) = Tδkeep
	δtail  *ΔTail // [](rev↑, []oid)
	tδkeep time.Duration

	// openers waiting for δtail.Head to become covering their at.
	δwait map[δwaiter]struct{} // set{(at, ready)}

	// XXX δtail/δwait -> Storage. XXX or -> Cache? (so it is not duplicated many times for many DB case)
}


// NewDB creates new database handle.
func NewDB(stor IStorage) *DB {
	// XXX db options?
	db := &DB{
		stor:  stor,
		δtail: NewΔTail(),
		δwait: make(map[δwaiter]struct{}),

		tδkeep: 10*time.Minute, // see δtail discussion
	}

	watchq := make(chan CommitEvent)
	stor.AddWatch(watchq)
	go db.watcher(watchq)
	// XXX DelWatch? in db.Close() ?

	return db
}

// ConnOptions describes options to DB.Open .
type ConnOptions struct {
	At     Tid  // if !0, open Connection bound to `at` view of database; not latest.
	NoSync bool // don't sync with storage to get its last tid.

	// don't put connection back into DB pool after transaction ends.
	//
	// This is low-level option that allows to inspect/use connection's
	// LiveCache after transaction finishes, and to manually resync the
	// connection onto another database view. See Connection.Resync for
	// details.
	NoPool bool
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

// δwaiter represents someone waiting for δtail.Head to become ≥ at.
type δwaiter struct {
	at    Tid
	ready chan struct{}
}

// watcher receives events about new committed transactions and updates δtail.
//
// it also wakes up δtail waiters.
func (db *DB) watcher(watchq <-chan CommitEvent) { // XXX err ?
	for {
		event, ok := <-watchq
		if !ok {
			fmt.Printf("db: watcher: close")
			// XXX wake up all waiters?
			return // closed
		}

		fmt.Printf("db: watcher <- %v", event)

		var readyv []chan struct{} // waiters that become ready

		db.mu.Lock()
		db.δtail.Append(event.Tid, event.Changev)
		for w := range db.δwait {
			if w.at <= event.Tid {
				readyv = append(readyv, w.ready)
				delete(db.δwait, w)
			}
		}

		// forget older δtail entries
		tcut := db.δtail.Head().Time().Add(-db.tδkeep)
		δcut := TidFromTime(tcut)
		db.δtail.ForgetBefore(δcut)

		db.mu.Unlock()

		// wakeup waiters outside of db.mu
		for _, ready := range readyv {
			fmt.Printf("db: watcher: wakeup %v", ready)
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
// until that transaction is complete(*).
//
// (*) unless NoPool option is used.
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
		head := Tid(0)

		if opt.NoSync {
			db.mu.Lock()
			head = db.δtail.Head()	// = 0 if δtail was not yet initialized with first event
			db.mu.Unlock()
		}

		// !NoSync or δtail !initialized
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

	db.mu.Lock()
	// unlock is either in resyncAndDBUnlock, or in db.openOrDBUnlock -> err

	conn, err := db.openOrDBUnlock(ctx, at, opt.NoPool)
	if err != nil {
		return nil, err
	}
	conn.resyncAndDBUnlock(txn, at)
	return conn, nil
}

// openOrDBUnlock is internal worker for Open.
//
// it returns either new connection, or connection from the pool.
// returned connection does not necessarily have .at=at, and have to go through .resync().
//
// must be called with db.mu locked.
// db.mu is unlocked on error.
func (db *DB) openOrDBUnlock(ctx context.Context, at Tid, noPool bool) (*Connection, error) {
	fmt.Printf("db.openx %s %v\t; δtail (%s, %s]\n", at, noPool, db.δtail.Tail(), db.δtail.Head())
	// NoPool connection - create one anew
	if noPool {
		conn := newConnection(db, at)
		conn.noPool = true
		return conn, nil
	}

retry:
	for {
		// check if we already have an exact match
		conn := db.get(at, at)
		if conn != nil {
			return conn, nil
		}

		// no exact match - let's try to find nearest
		δtail := db.δtail
		δhead := db.δtail.Head()

		// too far in the past, and we know there is no exact match
		// -> new historic connection.
		if at <= db.δtail.Tail() {
			return newConnection(db, at), nil
		}

		// δtail !initialized yet
		if db.δtail.Head() == 0 {
			// XXX δtail could be not yet initialized, but e.g. last_tid changed
			//     -> we have to wait for δtail not to loose just-released live cache
			return newConnection(db, at), nil
		}

		// we have some δtail coverage, but at is ahead of that.
		if at > δhead {
			// wait till δtail.head is up to date covering ≥ at,
			// and retry the loop (δtail.tail might go over at while we are waiting)
			δready := make(chan struct{})
			db.δwait[δwaiter{at, δready}] = struct{}{}
			db.mu.Unlock()

			select {
			case <-ctx.Done():
				// leave db.mu unlocked
				return nil, ctx.Err()

			case <-δready:
				// ok - δtail.head went over at; relock db and retry
				db.mu.Lock()
				continue retry
			}
		}

		// XXX note: vvv at start δtail.Tail is not covering first committed txn

		// at ∈ (δtail, δhead]	; try to get nearby idle connection or make a new one
		conn = db.get(δtail.Tail(), at)
		if conn == nil {
			conn = newConnection(db, at)
		}
		return conn, nil
	}
}

// Resync resyncs the connection onto different database view and transaction.
//
// Connection objects pinned in live cache are guaranteed to stay in live
// cache, even if maybe in ghost state (e.g. if they have to be invalidated due
// to database changes).
//
// Resync can be used several times.
//
// Resync must be used only under the following conditions:
//
//	- the connection was initially opened with NoPool flag;
//	- previous transaction, under which connection was previously
//	  opened/resynced, must be already complete;
//	- contrary to DB.Open, at cannot be 0.
//
// Note: new at can be both higher and lower than previous connection at.
func (conn *Connection) Resync(txn transaction.Transaction, at Tid) {
	if !conn.noPool {
		panic("Conn.Resync: connection was opened without NoPool flag")
	}
	if at == 0 {
		panic("Conn.Resync: resync to at=0 (auto-mode is valid only for DB.Open)")
	}

	conn.db.mu.Lock()
	conn.resyncAndDBUnlock(txn, at)
}

// resyncAndDBUnlock serves Connection.Resync and DB.Open .
//
// must be called with conn.db locked and unlocks it at the end.
func (conn *Connection) resyncAndDBUnlock(txn transaction.Transaction, at Tid) {
	db := conn.db

	if conn.txn != nil {
		db.mu.Unlock()
		panic("Conn.resync: previous transaction is not yet complete")
	}

	// upon exit, with all locks released, register conn to txn.
	defer func() {
		conn.at = at
		conn.txn = txn
		txn.RegisterSync((*connTxnSync)(conn))
	}()

	// conn.at == at - nothing to do (even if out of δtail coverage)
	if conn.at == at {
		db.mu.Unlock()
		return
	}

	// conn.at != at - have to invalidate objects in live cache.
	δtail := db.δtail
	δobj  := make(map[Oid]struct{}) // set(oid) - what to invalidate
	δall  := false                  // if we have to invalidate all objects

	// both conn.at and at are covered by δtail - we can invalidate selectively
	if (δtail.Tail() < conn.at && conn.at <= δtail.Head()) &&
	   (δtail.Tail() <      at &&      at <= δtail.Head()) {
		var δv []δRevEntry
		if conn.at <= at {
			δv = δtail.SliceByRev(conn.at, at)
		} else {
			// at < conn.at
			δv = δtail.SliceByRev(at-1, conn.at-1)
		}

		for _, δ := range δv {
			for _, oid := range δ.changev {
				δobj[oid] = struct{}{}
			}
		}

	// some of conn.at or at is outside δtail coverage - invalidate all
	// objects, but keep the objects present in live cache.
	} else {
		δall = true
	}

	// unlock db before locking cache and txn
	db.mu.Unlock()

	conn.cache.Lock()
	defer conn.cache.Unlock()

	if δall {
		// XXX keep synced with LiveCache details
		// XXX -> conn.cache.forEach?
		// XXX should we wait for db.stor.head to cover at?
		//     or leave this wait till .Load() time?
		for _, wobj := range conn.cache.objtab {
			obj, _ := wobj.Get().(IPersistent)
			if obj != nil {
				obj.PInvalidate()
			}
		}
	} else {
		for oid := range δobj {
			obj := conn.cache.Get(oid)
			if obj != nil {
				obj.PInvalidate()
			}
		}
	}

	// all done
	return
}

// get returns connection from db pool most close to at with conn.at ∈ [atMin, at].
//
// XXX recheck [atMin    or   (atMin
//
// if there is no such connection in the pool - nil is returned.
// must be called with db.mu locked.
func (db *DB) get(atMin, at Tid) *Connection {
	l := len(db.pool)

	// find pool index corresponding to at:
	// [i-1].at ≤ at < [i].at
	i := sort.Search(l, func(i int) bool {
		return at < db.pool[i].at
	})

	fmt.Printf("pool:\n")
	for i := 0; i < l; i++ {
		fmt.Printf("\t[%d]:  .at = %s\n", i, db.pool[i].at)
	}
	fmt.Printf("get  [%s, %s] -> %d\n", atMin, at, i)

	// search through window of X previous connections and find out the one
	// with minimal distance to get to state @at that fits into requested range.
	//
	// XXX search not only previous, but future too? (we can get back to
	// past by invalidating what was later changed) (but likely it will
	// hurt by destroying cache of more recent connection).
	const X = 10 // XXX search window size: hardcoded
	jδmin := -1
	for j := i - X; j < i; j++ {
		if j < 0 {
			continue
		}
		if db.pool[j].at < atMin {
			continue
		}

		// TODO search for max N(live) - N(live, that will need to be invalidated)
		jδmin = j // XXX stub (using rightmost j)
	}

	// nothing found
	if jδmin < 0 {
		return nil
	}

	// found - reuse the connection
	conn := db.pool[jδmin]
	copy(db.pool[jδmin:], db.pool[jδmin+1:])
	db.pool[l-1] = nil
	db.pool = db.pool[:l-1]

	if conn.db != db {
		panic("DB.get: foreign connection in the pool")
	}
	if conn.txn != nil {
		panic("DB.get: live connection in the pool")
	}

	return conn
}

// put puts connection back into db pool.
func (db *DB) put(conn *Connection) {
	if conn.db != db {
		panic("DB.put: conn.db != db")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// XXX check if len(pool) > X, and drop conn if yes
	// [i-1].at ≤ at < [i].at
	i := sort.Search(len(db.pool), func(i int) bool {
		return conn.at < db.pool[i].at
	})

	//db.pool = append(db.pool[:i], conn, db.pool[i:]...)
	db.pool = append(db.pool, nil)
	copy(db.pool[i+1:], db.pool[i:])
	db.pool[i] = conn

	// XXX GC too idle connections here? XXX
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

	// mark the connection as no longer being live
	conn.txn = nil

	if !conn.noPool {
		conn.db.put(conn)
	}
}
