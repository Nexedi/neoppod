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

package sqlite
// simple connection pool

import (
	"errors"
	"sync"

	"lab.nexedi.com/kirr/go123/xerr"

	sqlite3 "github.com/gwenn/gosqlite"
)

// connPool is a pool of sqlite3.Conn
type connPool struct {
	factory func() (*sqlite3.Conn, error) // =nil if pool closed

	mu     sync.Mutex
	connv  []*sqlite3.Conn // operated as stack
}

// newConnPool creates new connPool that will be using factory to create new
// connections.
func newConnPool(factory func() (*sqlite3.Conn, error)) *connPool {
	return &connPool{factory: factory}
}

// Close closes pool and all connections that were in it.
func (p *connPool) Close() error {
	p.mu.Lock()
	connv := p.connv
	p.connv = nil
	p.factory = nil
	p.mu.Unlock()

	var errv xerr.Errorv
	for _, conn := range connv {
		err := conn.Close()
		errv.Appendif(err)
	}

	return errv.Err()
}

var errClosedPool = errors.New("sqlite: pool: getConn on closed pool")

// getConn returns a connection - either from pool, or newly created if the
// pool was empty.
func (p *connPool) getConn() (conn *sqlite3.Conn, _ error) {
	p.mu.Lock()

	factory := p.factory
	if factory == nil {
		p.mu.Unlock()
		return nil, errClosedPool
	}

	if l := len(p.connv); l > 0 {
		l--
		conn = p.connv[l]
		p.connv[l] = nil  // just in case
		p.connv = p.connv[:l]
	}

	p.mu.Unlock()

	if conn != nil {
		return conn, nil
	}

	// pool was empty - we need to create new connection
	return factory()
}

// putConn puts a connection to pool.
//
// Caller must not directly use conn after call to putConn anymore.
func (p *connPool) putConn(conn *sqlite3.Conn) {
	p.mu.Lock()
	if p.factory != nil { // forgiving putConn after close
		p.connv = append(p.connv, conn)
	}
	p.mu.Unlock()

	// XXX Close conn that is put after pool.Close()
}
