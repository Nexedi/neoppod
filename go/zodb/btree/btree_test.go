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

//go:generate ./testdata/gen-testdata

import (
	"context"
	"testing"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/neo/go/transaction"
	"lab.nexedi.com/kirr/neo/go/zodb"
	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"
)

// kv is one (key, value) pair.
type kv struct {
	key   int64
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
	Get(context.Context, int64) (interface{}, bool, error)
}

func TestBTree(t *testing.T) {
	X := exc.Raiseif
	ctx := context.Background()
	stor, err := zodb.OpenStorage(ctx, "testdata/1.fs", &zodb.OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	db := zodb.NewDB(stor)

	txn, ctx := transaction.New(ctx)
	defer txn.Abort()

	conn, err := db.Open(ctx, &zodb.ConnOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// XXX close db/stor

	// go through small test Buckets/BTrees and verify that Get(key) is as expected.
	for _, tt := range smallTestv {
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
			if _, ok = obj.(*LOBucket); !ok {
				want = "Bucket"
			}
		case kindBTree:
			if _, ok = obj.(*LOBTree); !ok {
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
				t.Errorf("%s: get %v -> ø;  want %v", tt.oid, kv.key, kv.value)
				continue
			}

			if value != kv.value {
				t.Errorf("%s: get %v -> %v;  want %v", tt.oid, kv.key, value, kv.value)
			}

			// XXX .next == nil
			// XXX check keys, values directly (i.e. there is nothing else)
		}
	}


	// B3 is a large BTree with {i: i} data.
	// verify Get(key) and that different bucket links lead to the same in-RAM object.
	xB3, err := conn.Get(ctx, B3_oid)
	if err != nil {
		t.Fatal(err)
	}
	B3, ok := xB3.(*LOBTree)
	if !ok {
		t.Fatalf("B3: %v; got %T;  want BTree", B3_oid, xB3)
	}

	for i := int64(0); i <= B3_maxkey; i++ {
		v, ok, err := B3.Get(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("B3: get %v -> ø;  want %v", i, i)
		}
		if int64(i) != v {
			t.Fatalf("B3: get %v -> %v;  want %v", i, v, i)
		}
	}

	// verifyFirstBucket verifies that b.firstbucket is correct and returns it.
	var verifyFirstBucket func(b *LOBTree) *LOBucket
	verifyFirstBucket = func(b *LOBTree) *LOBucket {
		err := b.PActivate(ctx);	X(err)
		defer b.PDeactivate()

		var firstbucket *LOBucket

		switch child := b.data[0].child.(type) {
		default:
			t.Fatalf("btree(%s): child[0] is %T", b.POid(), b.data[0].child)

		case *LOBTree:
			firstbucket = verifyFirstBucket(child)

		case *LOBucket:
			firstbucket = child
		}

		if firstbucket != b.firstbucket {
			t.Fatalf("btree(%s): firstbucket -> %p (oid: %s); actual first bucket = %p (oid: %s)",
				b.POid(), b.firstbucket, b.firstbucket.POid(), firstbucket, firstbucket.POid())
		}

		return firstbucket
	}

	verifyFirstBucket(B3)
}
