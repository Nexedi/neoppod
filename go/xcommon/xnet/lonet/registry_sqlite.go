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
// registry implemented as shared SQLite file

import (
	"context"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqliteutil"
)

// registry schema
//
// hosts:
//	hostname	text !null PK
//	osladdr		text !null
//
// meta:
//	name		text !null PK
//	value		text !null
//
// "schemaver"	str(int) - version of schema.
// "network"    text     - name of lonet network this registry serves.

type sqliteRegistry struct {
	dbpool *sqlite.Pool

	uri    string	// URI db was originally opened with
}

// openRegistrySqlite opens SQLite registry located at dburi.
// XXX network name?
func openRegistrySQLite(ctx context.Context, dburi string) (_ *sqliteRegistry, err error) {
	r := &sqliteRegistry{uri: dburi}
	defer r.regerr(&err, "open")

	dbpool, err := sqlite.Open(dburi, 0, /* poolSize= */16)	// XXX pool size ok?
	if err != nil {
		return nil, err
	}

	r.dbpool = dbpool

	err = r.setup(ctx)
	if err != nil {
		r.Close()
		return nil, err
	}

	return r, nil
}

func (r *sqliteRegistry) Close() (err error) {
	defer r.regerr(&err, "close")
	return r.dbpool.Close()
}

func (r *sqliteRegistry) setup(ctx context.Context) (err error) {
	// XXX dup
	conn := r.dbpool.Get(ctx.Done())
	if conn == nil {
		// either ctx cancel or dbpool close
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errRegistryDown // db closed
	}
	defer r.dbpool.Put(conn)

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

	// XXX check schemaver
	// XXX check network name

	return nil
}

func (r *sqliteRegistry) Announce(ctx context.Context, hostname, osladdr string) (err error) {
	defer r.regerr(&err, "announce", hostname, osladdr)

	// XXX dup
	conn := r.dbpool.Get(ctx.Done())
	if conn == nil {
		// either ctx cancel or dbpool close
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errRegistryDown // db closed
	}
	defer r.dbpool.Put(conn)

	err = sqliteutil.Exec(conn, "INSERT INTO hosts (hostname, osladdr) VALUES (?, ?)", nil, hostname, osladdr)

	switch sqlite.ErrCode(err) {
	case sqlite.SQLITE_CONSTRAINT_UNIQUE:	// XXX test
		err = errHostDup
	}

	return err
}

func (r *sqliteRegistry) Query(ctx context.Context, hostname string) (osladdr string, err error) {
	defer r.regerr(&err, "query", hostname)

	// XXX dup
	conn := r.dbpool.Get(ctx.Done())
	if conn == nil {
		// either ctx cancel or dbpool close
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", errRegistryDown // db closed
	}
	defer r.dbpool.Put(conn)

	err = sqliteutil.Exec(conn, "SELECT osladdr FROM hosts WHERE hostname = ?", func (stmt *sqlite.Stmt) error {
		osladdr = stmt.ColumnText(0)
		return nil
	})

	/* XXX reenable
	switch sqlite.ErrCode(err) {
	case sqlite.XXXNOROW:
		err = errNoHost
	}
	*/

	/*
	if err != nil {
		return "", err
	}
	*/

	return osladdr, err
}

// regerr is syntatic sugar to wrap !nil *errp into registryError.
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
