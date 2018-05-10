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

package lonet
// registry implemented as shared SQLite file.

import (
	"context"
	"errors"
	"fmt"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqliteutil"

	"lab.nexedi.com/kirr/go123/xerr"
)

// registry schema	(keep in sync wrt .setup())
//
// hosts:
//	hostname	text !null PK
//	osladdr		text !null
//
// meta:
//	name		text !null PK
//	value		text !null
//
// "schemaver"  text	- version of db schema.
// "network"    text	- name of lonet network this registry serves.

const schemaVer = "lonet.1"

type sqliteRegistry struct {
	dbpool *sqlite.Pool

	uri    string	// URI db was originally opened with
}

// openRegistrySQLite opens SQLite registry located at dburi.
//
// the registry is setup/verified to be serving specified lonet network.
func openRegistrySQLite(ctx context.Context, dburi, network string) (_ *sqliteRegistry, err error) {
	r := &sqliteRegistry{uri: dburi}
	defer r.regerr(&err, "open")

	dbpool, err := sqlite.Open(dburi, 0, /* poolSize= */16)	// XXX pool size ok?
	if err != nil {
		return nil, err
	}

	r.dbpool = dbpool

	// initialize/check db
	err = r.setup(ctx, network)
	if err != nil {
		r.Close()
		return nil, err
	}

	return r, nil
}

// Close implements registry.
func (r *sqliteRegistry) Close() (err error) {
	defer r.regerr(&err, "close")
	return r.dbpool.Close()
}

// withConn runs f on a dbpool connection.
//
// connection is first allocated from dbpool and put back after call to f.
func (r *sqliteRegistry) withConn(ctx context.Context, f func(*sqlite.Conn) error) error {
	conn := r.dbpool.Get(ctx.Done())
	if conn == nil {
		// either ctx cancel or dbpool close
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errRegistryDown // db closed
	}
	defer r.dbpool.Put(conn)
	return f(conn)
}

var errNoRows   = errors.New("query: empty result")
var errManyRows = errors.New("query: multiple results")

// query1 is like sqliteutil.Exec but checks that exactly 1 row is returned.
//
// if query results in no rows - errNoRows is returned.
// if query results in more than 1 row - errManyRows is returned.
func query1(conn *sqlite.Conn, query string, resultf func(stmt *sqlite.Stmt), argv ...interface{}) error {
	nrow := 0
	err := sqliteutil.Exec(conn, query, func(stmt *sqlite.Stmt) error {
		if nrow == 1 {
			return errManyRows
		}
		nrow++
		resultf(stmt)
		return nil
	}, argv...)
	if err != nil {
		return err
	}
	if nrow == 0 {
		return errNoRows
	}
	return nil
}

// setup initializes/checks registry database to be of expected schema and configuration.
func (r *sqliteRegistry) setup(ctx context.Context, network string) (err error) {
	defer xerr.Contextf(&err, "setup %q", network)

	return r.withConn(ctx, func(conn *sqlite.Conn) (err error) {
		// NOTE: keep in sync wrt top-level text.
		err = sqliteutil.ExecScript(conn, `
			CREATE TABLE IF NOT EXISTS hosts (
				hostname	TEXT NON NULL PRIMARY KEY,
				osladdr		TEXT NON NULL
			);

			CREATE TABLE IF NOT EXISTS meta (
				name		TEXT NON NULL PRIMARY KEY,
				value		TEXT NON NULL
			);
		`)
		if err != nil {
			return err
		}

		// do whole checks/init under transaction, so that there is
		// e.g. no race wrt another process setting config.
		defer sqliteutil.Save(conn)(&err)

		// check/init schema version
		ver, err := r.config(conn, "schemaver")
		if err != nil {
			return err
		}
		if ver == "" {
			ver = schemaVer
			err = r.setConfig(conn, "schemaver", ver)
			if err != nil {
				return err
			}
		}
		if ver != schemaVer {
			return fmt.Errorf("schema version mismatch: want %q; have %q", schemaVer, ver)
		}

		// check/init network name
		dbnetwork, err := r.config(conn, "network")
		if err != nil {
			return err
		}
		if dbnetwork == "" {
			dbnetwork = network
			err = r.setConfig(conn, "network", dbnetwork)
			if err != nil {
				return err
			}
		}
		if dbnetwork != network {
			return fmt.Errorf("network name mismatch: want %q; have %q", network, dbnetwork)
		}


		return nil
	})
}

// config gets one registry configuration value by name.
//
// if there is no record corresponding to name - ("", nil) is returned.
// XXX add ok ret to indicate presence of value?
func (r *sqliteRegistry) config(conn *sqlite.Conn, name string) (value string, err error) {
	defer xerr.Contextf(&err, "config: get %q", name)

	err = query1(conn, "SELECT value FROM meta WHERE name = ?", func(stmt *sqlite.Stmt) {
		value = stmt.ColumnText(0)
	}, name)

	switch err {
	case errNoRows:
		return "", nil
	case errManyRows:
		value = ""
	}

	return value, err
}

// setConfig sets one registry configuration value by name.
func (r *sqliteRegistry) setConfig(conn *sqlite.Conn, name, value string) (err error) {
	defer xerr.Contextf(&err, "config: set %q = %q", name, value)

	err = sqliteutil.Exec(conn,
		"INSERT OR REPLACE INTO meta (name, value) VALUES (?, ?)", nil,
		name, value)
	return err
}

// Announce implements registry.
func (r *sqliteRegistry) Announce(ctx context.Context, hostname, osladdr string) (err error) {
	defer r.regerr(&err, "announce", hostname, osladdr)

	return r.withConn(ctx, func(conn *sqlite.Conn) error {
		err := sqliteutil.Exec(conn,
			"INSERT INTO hosts (hostname, osladdr) VALUES (?, ?)", nil,
			hostname, osladdr)

		switch sqlite.ErrCode(err) {
		case sqlite.SQLITE_CONSTRAINT_UNIQUE:
			err = errHostDup
		}

		return err
	})
}

var errRegDup = errors.New("registry broken: duplicate host entries")

// Query implements registry.
func (r *sqliteRegistry) Query(ctx context.Context, hostname string) (osladdr string, err error) {
	defer r.regerr(&err, "query", hostname)

	err = r.withConn(ctx, func(conn *sqlite.Conn) error {
		err := query1(conn, "SELECT osladdr FROM hosts WHERE hostname = ?",
			func (stmt *sqlite.Stmt) {
				osladdr = stmt.ColumnText(0)
			}, hostname)

		switch err {
		case errNoRows:
			return errNoHost

		case errManyRows:
			// hostname is PK - we should not get several results
			osladdr = ""
			return errRegDup
		}

		return err
	})

	return osladdr, err
}

// regerr is syntactic sugar to wrap !nil *errp into registryError.
//
// intended too be used like
//
//	defer r.regerr(&err, "operation", arg1, arg2, ...)
func (r *sqliteRegistry) regerr(errp *error, op string, args ...interface{}) {
	if *errp == nil {
		return
	}

	// XXX name of the network
	*errp = &registryError{
		Registry: r.uri,
		Op:       op,
		Args:     args,
		Err:      *errp,
	}
}
