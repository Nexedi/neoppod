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

// Package sql provides NEO storage backend that uses SQL database for persistence.
package sql

// TODO also support mysql

import (
	"context"
	"net/url"

	"lab.nexedi.com/kirr/go123/mem"

	"lab.nexedi.com/kirr/neo/go/neo/storage"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"database/sql"
        _ "github.com/mattn/go-sqlite3"
	_ "github.com/go-sql-driver/mysql"
)


type SQLBackend struct {
	db *sql.DB
}

var _ storage.Backend = (*SQLBackend)(nil)

func (b *SQLBackend) LastTid(ctx context.Context) (zodb.Tid, error) {
	panic("TODO")
}

func (b *SQLBackend) LastOid(ctx context.Context) (zodb.Oid, error) {
	panic("TODO")
}

func (b *SQLBackend) Load(ctx context.Context, xid zodb.Xid) (*mem.Buf, zodb.Tid, zodb.Tid, error) {
	panic("TODO")
}


// ---- open by URL ----

func openURL(ctx context.Context, u *url.URL) (storage.Backend, error) {
	// TODO handle query
	// XXX u.Path is not always raw path - recheck and fix
	path := u.Host + u.Path

	db, err := sql.Open("sqlite3", path)	// XXX +context
	if err != nil {
		return nil, err
	}

	// check we can actually access db
	err = db.PingContext(ctx)
	if err != nil {
		// XXX db.Close()
		return nil, err	// XXX err ctx
	}

	return nil, nil
}


func init() {
	storage.RegisterBackend("sqlite", openURL)
}
