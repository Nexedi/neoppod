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

package btree

//go:generate ./py/gen-testdata

import (
	"context"
	"testing"

	"lab.nexedi.com/kirr/neo/go/transaction"
	"lab.nexedi.com/kirr/neo/go/zodb"
	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"
)

// kv is one (key, value) pair.
type kv struct {
	key   KEY
	value interface{}
}

// testEntry is information about 1 Bucket or BTree (XXX) object.
type testEntry struct {
	oid   zodb.Oid
	itemv []kv
}

func TestBucket(t *testing.T) {
	ctx := context.Background()
	stor, err := zodb.OpenStorage(ctx, "testdata/1.fs", &zodb.OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	db := zodb.NewDB(stor)

	txn, ctx := transaction.New(ctx)
	defer txn.Abort()

	conn, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range _1fs_testEntry {
		xobj, err := conn.Get(ctx, tt.oid)
		if err != nil {
			t.Fatal(err)
		}

		obj, ok := xobj.(*Bucket)
		if !ok {
			t.Fatalf("%s: got %T;  want Bucket", tt.oid, xobj)
		}

		_ = obj
	}
}
