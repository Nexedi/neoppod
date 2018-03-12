// Copyright (C) 2018  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//                     schema & queries are based on neo/storage/database/sqlite.py
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
	"errors"
	"fmt"
	"net/url"
	"strings"
	//"reflect"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/go123/xerr"

	"lab.nexedi.com/kirr/neo/go/neo/proto"
	"lab.nexedi.com/kirr/neo/go/neo/storage"
	"lab.nexedi.com/kirr/neo/go/zodb"

	// NOTE github.com/gwenn/gosqlite is used for the following reasons:
	//
	// - it is used directly instead of using it via "database/sql" because for a
	//   typical 5µs query quering through "database/sql", even in the most
	//   careful, hacky and unsafe way, adds at least 3µs and more.
	//   see also: https://github.com/golang/go/issues/23879
	//
	// - "github.com/mattn/go-sqlite3" does not provide a good way to Scan
	//   queries made directly.
	//
	// we need to do only simple queries and thus do not use any Cgo->Go
	// callback-related functionality from github.com/gwenn/gosqlite. This
	// way it should be safe for us to use it even without GODEBUG=cgocheck=0.
	//
	// --------
	//
	// NOTE 2: we do not interrupt requests on context cancelation:
	//
	// - it is relatively expensive to support when using a CGo library - see e.g.
	//   https://github.com/mattn/go-sqlite3/pull/530
	//   https://github.com/golang/go/issues/19574#issuecomment-366513872
	//
	// - on Linux disk file IO, in contrast to e.g. network and pipes,
	//   cannot be really interrupted.
	//
	// so we are ok for the cancel to be working on the granualarity of
	// whole query.
	sqlite3 "github.com/gwenn/gosqlite"
)

// ---- schema ----

const schemaVersion = 2

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
						-- XXX ^^^ can be NOT NULL with 0 serving instead

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
	pool *connPool
	url  string
}

var _ storage.Backend = (*Backend)(nil)

// row1 is like sql.Row to Scan values once and then put stmt and conn back to their pools.
type row1 struct {
	pool *connPool
	conn *sqlite3.Conn
	stmt *sqlite3.Stmt
	err  error		// != nil on an error obtaining the row
}

var errNoRows = errors.New("sqlite: no rows in result set")

func (r *row1) Scan(argv ...interface{}) error {
	if r.pool == nil {
		panic("sqlite: row1: .Scan called second time")
	}

	err := r.err
	if err == nil {
		err = r.stmt.Scan(argv...)
	}

	if r.stmt != nil {
		err2 := r.stmt.Reset() // else it won't be put back to cache
		if err == nil {
			err = err2
		}

		err2 = r.stmt.Finalize() // -> conn's stmt cache
		if err == nil {
			err = err2
		}

		r.stmt = nil // just in case
	}

	if r.conn != nil {
		r.pool.putConn(r.conn)
		r.conn = nil
	}

	// to catch double .Scan
	r.pool = nil

	return err
}

// query1 performs 1 select-like query.
//
// the result needs to be .Scan'ned once similarly to how it is done in database/sql.
func (b *Backend) query1(query string, argv ...interface{}) *row1 {
	row := &row1{pool: b.pool}

	// pool -> conn
	conn, err := b.pool.getConn()
	if err != nil {
		row.err = err
		return row
	}
	row.conn = conn

	// conn -> stmt
	stmt, err := conn.Prepare(query) // uses conn's stmt cache
	if err != nil {
		row.err = err
		return row
	}
	row.stmt = stmt

	// stmt += argv
	err = stmt.Bind(argv...)
	if err != nil {
		row.err = err
		return row
	}

	// everything prepared - run the query
	ok, err := stmt.Next()
	if err != nil {
		row.err = err
		return row
	}

	if !ok {
		row.err = errNoRows
	}

	return row
}



func (b *Backend) LastTid(ctx context.Context) (zodb.Tid, error) {
	var lastTid zodb.Tid

	// FIXME nodeID <- my node UUID
	myID := proto.UUID(proto.STORAGE, 1)

	err := b.query1("SELECT MAX(tid) FROM pt, trans" +
			" WHERE nid=? AND rid=partition" /* XXX AND tid<=? (max_tid) */,
			myID).Scan(&lastTid)

	if err != nil {
		// no transaction have been committed
		if err == errNoRows {
			return 0, nil
		}

		return 0, &zodb.OpError{URL: b.url, Op: "last_tid", Err: err}
	}

	return lastTid, nil
}

func (b *Backend) LastOid(ctx context.Context) (zodb.Oid, error) {
	var lastOid zodb.Oid

	// FIXME nodeID <- my node UUID
	myID := proto.UUID(proto.STORAGE, 1)

	err := b.query1("SELECT MAX(oid) FROM pt, obj WHERE nid=? AND rid=partition",
			myID).Scan(&lastOid)

	if err != nil {
		// no objects
		if err == errNoRows {
			return proto.INVALID_OID, nil
		}

		return 0, &zodb.OpError{URL: b.url, Op: "last_oid", Err: err}
	}

	return lastOid, nil
}

func (b *Backend) Load(ctx context.Context, xid zodb.Xid) (_ *proto.AnswerObject, err error) {
	defer func() {
		if err != nil {
			err = &zodb.OpError{URL: b.url, Op: "load", Err: err}
		}
	}()

	obj := &proto.AnswerObject{Oid: xid.Oid, DataSerial: 0}
	// TODO reenable, but XXX we have to use Query, not QueryRow for RawBytes support
	//var data sql.RawBytes
	var data []byte

	// XXX recheck vvv with sqlite3 direct
	// hash is variable-length BLOB - Scan refuses to put it into [20]byte
	//var hash sql.RawBytes
	var hash []byte

	// obj.value_tid can be null
	//var valueTid sql.NullInt64	// XXX ok not to uint64 - max tid is max signed int64
	var valueTid int64	// XXX ok not to uint64 - max tid is max signed int64

	// FIXME pid = getReadablePartition (= oid % Np; error if pid not readable)
	pid := 0

	// XXX somehow detect errors in sql misuse and log them as 500 without reporting to client?
	// XXX such errors start with "unsupported Scan, "

	// XXX use conn for severl query1 (see below) without intermediate returns to pool?

	err = b.query1(
		"SELECT tid, compression, data.hash, value, value_tid" +
		" FROM obj LEFT JOIN data ON obj.data_id = data.id" +
		" WHERE partition=? AND oid=? AND tid<=?" +
		" ORDER BY tid DESC LIMIT 1",
		pid, xid.Oid, xid.At).
		Scan(&obj.Serial, &obj.Compression, &hash, &data, &valueTid)

	if err != nil {
		if err == errNoRows {
			// nothing found - check whether object exists at all
			var __ zodb.Oid
			err = b.query1(
				"SELECT oid FROM obj WHERE partition=? AND oid=? LIMIT 1",
				pid, xid.Oid) .Scan(&__)

			switch {
			case err == nil:
				err = &zodb.NoDataError{
					Oid:	   xid.Oid,
					DeletedAt: 0,		// XXX hardcoded
				}

			case err == errNoRows:
				err = &zodb.NoObjectError{Oid: xid.Oid}

			}
		}

		return nil, err
	}

	// hash -> obj.Checksum
	if len(hash) != len(obj.Checksum) {
		return nil, fmt.Errorf("data corrupt: len(hash) = %d", len(hash))
	}
	copy(obj.Checksum[:], hash)

	// valueTid -> obj.DataSerial
	if valueTid != 0 {
		obj.DataSerial = zodb.Tid(valueTid)
	}


	// data -> obj.Data
	obj.Data = mem.BufAlloc(len(data))
	copy(obj.Data.Data, data)

	// find out nextSerial
	// XXX kill nextSerial support after neo/py cache does not need it
	err = b.query1(
		"SELECT tid from obj" +
		" WHERE partition=? AND oid=? AND tid>?" +
		" ORDER BY tid LIMIT 1",
		pid, xid.Oid, xid.At).
		Scan(&obj.NextSerial)

	if err != nil {
		if err == errNoRows {
			obj.NextSerial = proto.INVALID_TID
		} else {
			return nil, err
		}
	}

	return obj, nil
}


func (b *Backend) config(key string, pvalue *string) error {
	return b.query1("SELECT value FROM config WHERE name=?", key).Scan(pvalue)
}

func (b *Backend) Close() error {
	err := b.pool.Close()
	return err	// XXX err ctx
}

// ---- open by URL ----

func openURL(ctx context.Context, u *url.URL) (_ storage.Backend, err error) {
	url := u.String()
	dburl := strings.TrimPrefix(url, u.Scheme+"://") // url with stripped sqlite://
	connFactory := func() (*sqlite3.Conn, error) {
		return sqlite3.Open(dburl)
	}

	b := &Backend{pool: newConnPool(connFactory), url: url}

	defer func() {
		if err != nil {
			b.Close()
		}
	}()

	// check we can actually access db
	conn, err := b.pool.getConn()
	if err == nil {
		err = conn.Close()
	}
	if err != nil {
		return nil, err
	}

	// check schema and that our limited version can work with the db
	// (by making some queries in open we also check whether we can access db at all)
	errv := xerr.Errorv{}
	checkConfig := func(name string, expect interface{}) {
		//pvalue := reflect.New(reflect.TypeOf(expect)).Interface()
		value := ""
		err := b.config(name, &value)

		// XXX prefix "b.path: config: %s:"

		switch err {
		case errNoRows:
			err = fmt.Errorf("not found")
		case nil:
			//value := reflect.ValueOf(pvalue).Elem().Interface()
			sexpect := fmt.Sprintf("%v", expect)
			if value != sexpect {
				err = fmt.Errorf("got %v; want %v", value, sexpect)
			}
		}

		if err != nil {
			errv.Appendf("%s: config: %s: %s", b.url, name, err)
		}
	}

	checkConfig("version",		schemaVersion)
	checkConfig("nid",		int(proto.UUID(proto.STORAGE, 1)))
	checkConfig("partitions",	1)
	checkConfig("replicas",		0)	// XXX neo/py uses nreplicas as 1 + n(replica)

	err = errv.Err()
	if err != nil {
		return nil, fmt.Errorf("NEO/go POC: not ready to handle: %s", err)
	}

	// config("version")
	// config("nid")
	// config("partitions")
	// config("replicas")
	// config("name")
	// config("ptid")
	// config("backup_tid")
	// config("truncate_tid")
	// config("_pack_tid")

	// check ttrans/tobj to be empty - else there are some unfinished, or
	// not-yet-moved to trans/tobj transactions.
	nttrans, ntobj := 0, 0
	errv = xerr.Errorv{}
	errv.Appendif( b.query1("SELECT COUNT(*) FROM ttrans") .Scan(&nttrans) )
	errv.Appendif( b.query1("SELECT COUNT(*) FROM tobj")   .Scan(&ntobj) )

	err = errv.Err()
	if err != nil {
		return nil, fmt.Errorf("NEO/go POC: checking ttrans/tobj: %s", err)
	}

	if !(nttrans==0 && ntobj==0) {
		return nil, fmt.Errorf("NEO/go POC: not ready to handle: !empty ttrans/tobj")
	}

	// TODO lock db by path so other process cannot start working with it

	return b, nil
}


func init() {
	storage.RegisterBackend("sqlite", openURL)
}
