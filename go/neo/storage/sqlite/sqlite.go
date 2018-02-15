// Copyright (C) 2018  Nexedi SA and Contributors.
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

// Package sqlite provides NEO storage backend that uses SQLite database for persistence.
package sqlite

import (
	"context"
	"net/url"

	"lab.nexedi.com/kirr/go123/mem"

	"lab.nexedi.com/kirr/neo/go/neo/proto"
	"lab.nexedi.com/kirr/neo/go/neo/storage"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"database/sql"
        _ "github.com/mattn/go-sqlite3"
)

const version = 2

// ---- schema ----

// table "config" stores configuration parameters which affect the persistent data.
//
// XXX
// (name, nid, partitions, ptid, replicas, version, zodb=pickle...)
const config = `
	name	TEXT NOT NULL PRIMARY KEY,
	value	TEXT
`

// table "pt" stores a partition table.
const pt = `
	rid	INTEGER NOT NULL,	-- row id
	nid	INTEGER NOT NULL,	-- node id
	state	INTEGER NOT NULL,	-- cell state

	PRIMARY KEY (rid, nid)
`

// table "trans" stores information on committed transactions.
const trans = `
	partition	INTEGER NOT NULL,
	tid		INTEGER NOT NULL,
	packed		BOOLEAN NOT NULL,
	oids		BLOB NOT NULL,		-- []oid
	user		BLOB NOT NULL,
	description	BLOB NOT NULL,
	ext		BLOB NOT NULL,
	ttid		INTEGER NOT NULL,

	PRIMARY KEY (partition, tid)
`

// table "obj" stores committed object metadata.
const obj = `
	partition	INTEGER NOT NULL,
	oid		INTEGER NOT NULL,
	tid		INTEGER NOT NULL,
	data_id		INTEGER,		-- -> data.id
	value_tid	INTEGER,		-- data_tid for zodb
	
	PRIMARY KEY (partition, oid, tid)
`
//	`(partition, tid, oid)`
//	`(data_id)`

// XXX reenable for ^^^
//index_dict['obj'] = (
//    "CREATE INDEX %s ON %s(partition, tid, oid)",
//    "CREATE INDEX %s ON %s(data_id)")

// table "data" stores object data.
const data = `
	id		INTEGER PRIMARY KEY,
	hash		BLOB NOT NULL,
	compression	INTEGER NOT NULL,
	value		BLOB NOT NULL
`
// XXX reenable for ^^^
//if dedup:
//    index_dict['data'] = (
//        "CREATE UNIQUE INDEX %s ON %s(hash, compression)",)


// table "ttrans" stores information on uncommitted transactions.
const ttrans = `
	partition	INTEGER NOT NULL,
	tid		INTEGER,
	packed		BOOLEAN NOT NULL,
	oids		BLOB NOT NULL,
	user		BLOB NOT NULL,
	description	BLOB NOT NULL,
	ext		BLOB NOT NULL,
	ttid		INTEGER NOT NULL
`

// table "tobj" stores uncommitted object metadata.
const tobj = `
	partition	INTEGER NOT NULL,
	oid		INTEGER NOT NULL,
	tid		INTEGER NOT NULL,
	data_id		INTEGER,
	value_tid	INTEGER,

	PRIMARY KEY (tid, oid)
`





type Backend struct {
	db  *sql.DB
	url string
}

var _ storage.Backend = (*Backend)(nil)

func (b *Backend) query1(ctx context.Context, query string, argv ...interface{}) *sql.Row {
	return b.db.QueryRowContext(ctx, query, argv...)
}

func (b *Backend) LastTid(ctx context.Context) (zodb.Tid, error) {
	var lastTid zodb.Tid
	err := b.query1(ctx,
		"SELECT MAX(tid) FROM pt, trans" +
		" WHERE nid=? AND rid=partition" /* XXX AND tid<=? (max_tid) */,
		b.nodeID()).Scan(&lastTid)

	if err != nil {
		// no transaction have been committed
		if err == sql.ErrNoRows {
			return 0, nil
		}

		// XXX ok to reuse zodb.OpError here? or better it should be storage.OpError ?
		return 0, &zodb.OpError{URL: b.url, Op: "last_tid", Err: err}
	}

	return lastTid, nil
}

func (b *Backend) LastOid(ctx context.Context) (zodb.Oid, error) {
	panic("TODO")
}

func (b *Backend) Load(ctx context.Context, xid zodb.Xid) (*proto.AnswerObject, error) {
	// XXX err ctx zodb.OpError{URL: b.url, Op: "load", Err: ...}
	obj := &proto.AnswerObject{Oid: xid.Oid}
	var data sql.RawBytes

	// XXX pid = getReadablePartition (= oid % Np, raise if pid not readable)
	err := b.query1(ctx,
		"SELECT tid, compression, data.hash, value, value_tid" +
		" FROM obj LEFT JOIN data ON obj.data_id = data.id" +
		" WHERE partition=? AND oid=? AND tid<=?" +
		" ORDER BY tid DESC LIMIT 1",
		pid, xid.Oid, xid.At).
		Scan(&obj.Serial, &obj.Compression, &obj.Checksum, &data, &obj.DataSerial)

	if err != nil {
		if err == sql.ErrNoRows {
			// XXX see if object exists at all
			err = &zodb.NoDataError{
				Oid:	   xid.Oid,
				DeletedAt: 0,		// XXX hardcoded
			}

			err = &zodb.NoObjectError{Oid: xid.Oid}
		}

		return nil, err
	}

	// data -> obj.Data
	obj.Data = mem.BufAlloc(len(data))
	copy(obj.Data.Data, data)

	// find out nextSerial
	// XXX kill nextSerial support after neo/py cache does not need it
	err = b.query1(ctx,
		"SELECT tid from obj" +
		" WHERE partition=? AND oid=? AND tid>?" +
		" ORDER BY tid LIMIT 1",
		pid, xid.Oid, xid.At).
		Scan(&obj.NextSerial)

	if err != nil {
		if err == sql.ErrNoRows {
			obj.NextSerial = proto.INVALID_TID
		} else {
			return nil, err
		}
	}

	return obj, nil
}


/*
func (b *Backend) config(key string) (..., error) {
	// XXX cache
	var value string

	err := b.query1("SELECT value FROM config WHERE name=?", key).Scan(&value)
	if err != nil {
		if err = sql.ErrNoRows {
			// XXX
		}

		// XXX
	}

	return value, nil
}
*/

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

	// XXX check config("version") vs version
	// config("nid")
	// config("partitions")
	// config("replicas")
	// config("name")
	// config("ptid")
	// config("backup_tid")
	// config("truncate_tid")
	// config("_pack_tid")

	return &Backend{db: db, url: u.String()}, nil
}


func init() {
	storage.RegisterBackend("sqlite", openURL)
}
