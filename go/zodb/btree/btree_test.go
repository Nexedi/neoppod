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

type bkind int
const (
	kindBucket bkind = iota
	kindBTree
)

// testEntry is information about a Bucket or a BTree.
type testEntry struct {
	oid   zodb.Oid
	kind  bkind
	itemv []kv
}

// bmapping represents Get of Bucket or BTree.
type bmapping interface {
	Get(context.Context, KEY) (interface{}, bool, error)
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

	for _, tt := range _bucketTestv {
		xobj, err := conn.Get(ctx, tt.oid)
		if err != nil {
			t.Fatal(err)
		}

		obj, ok := xobj.(bmapping)
		if !ok {
			t.Fatalf("%s: got %T;  want Bucket|BTree", tt.oid, xobj)
		}

		want := ""
		switch tt.kind {
		case kindBucket:
			if _, ok = obj.(*Bucket); !ok {
				want = "Bucket"
			}
		case kindBTree:
			if _, ok = obj.(*BTree); !ok {
				want = "BTree"
			}
		default:
			panic(0)
		}

		if want != "" {
			t.Fatalf("%s: got %T;  want %s", tt.oid, obj, want)
		}

		for _, kv := range tt.itemv {
			value, ok, err := obj.Get(ctx, kv.key)
			if err != nil {
				t.Error(err)
				continue
			}

			if !ok {
				t.Errorf("get %v -> ø;  want %v", kv.key, kv.value)
				continue
			}

			if value != kv.value {
				t.Errorf("get %v -> %v;  want %v", kv.key, value, kv.value)
			}

			// XXX .next == nil
			// XXX check keys, values directly (i.e. there is nothing else)
		}
	}
}
