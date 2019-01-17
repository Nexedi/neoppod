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

package btree

//go:generate ./testdata/gen-testdata

import (
	"context"
	"testing"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/xerr"
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

// bmapping represents LOBTree or a LOBucket wrapper.
type bmapping interface {
	Get(context.Context, int64) (interface{}, bool, error)
	MinKey(context.Context) (int64, bool, error)
	MaxKey(context.Context) (int64, bool, error)
}

// bucketWrap is syntatic sugar to automatically activate/deactivate a bucket on Get/{Min,Max}Key calls.
type bucketWrap LOBucket

func (b *bucketWrap) bucket() *LOBucket {
	return (*LOBucket)(b)
}

// withBucket runs f with b.LOBucket activated.
func (b *bucketWrap) withBucket(ctx context.Context, f func()) error {
	err := b.bucket().PActivate(ctx)
	if err != nil {
		return err
	}
	defer b.bucket().PDeactivate()

	f()
	return nil
}

func (b *bucketWrap) Get(ctx context.Context, key int64) (v interface{}, ok bool, err error) {
	err = b.withBucket(ctx, func() {
		v, ok = b.bucket().get(key)	// XXX -> Get
	})
	return
}

func (b *bucketWrap) MinKey(ctx context.Context) (k int64, ok bool, err error) {
	err = b.withBucket(ctx, func() {
		k, ok = b.bucket().MinKey()
	})
	return
}

func (b *bucketWrap) MaxKey(ctx context.Context) (k int64, ok bool, err error) {
	err = b.withBucket(ctx, func() {
		k, ok = b.bucket().MaxKey()
	})
	return
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

		want := ""
		switch tt.kind {
		case kindBucket:
			bobj, ok := xobj.(*LOBucket)
			if !ok {
				want = "LOBucket"
			} else {
				// bucket wrapper that accepts ctx on Get, MinKey etc
				xobj = (*bucketWrap)(bobj)
			}
		case kindBTree:
			if _, ok := xobj.(*LOBTree); !ok {
				want = "LOBTree"
			}
		default:
			panic(0)
		}

		if want != "" {
			t.Fatalf("%s: got %T;  want %s", tt.oid, xobj, want)
		}

		obj := xobj.(bmapping)

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

		// {Min,Max}Key
		kmin, okmin, emin := obj.MinKey(ctx)
		kmax, okmax, emax := obj.MaxKey(ctx)
		if err := xerr.Merge(emin, emax); err != nil {
			t.Errorf("%s: min/max key: %s", tt.oid, err)
			continue
		}

		ok := false
		ka, kb := int64(0), int64(0)
		if l := len(tt.itemv); l != 0 {
			ok = true
			ka = tt.itemv[0].key
			kb = tt.itemv[l-1].key
		}

		if !(kmin == ka && kmax == kb && okmin == ok && okmax == ok) {
			t.Errorf("%s: min/max key wrong: got [%v, %v] (%v, %v);  want [%v, %v] (%v, %v)",
				tt.oid, kmin, kmax, okmin, okmax, ka, kb, ok, ok)
		}
	}


	// B3 is a large BTree with {i: i} data.
	// verify Get(key), {Min,Max}Key and that different bucket links lead to the same in-RAM object.
	xB3, err := conn.Get(ctx, B3_oid)
	if err != nil {
		t.Fatal(err)
	}
	B3, ok := xB3.(*LOBTree)
	if !ok {
		t.Fatalf("B3: %v; got %T;  want LOBTree", B3_oid, xB3)
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

	kmin, okmin, emin := B3.MinKey(ctx)
	kmax, okmax, emax := B3.MaxKey(ctx)
	if err := xerr.Merge(emin, emax); err != nil {
		t.Fatalf("B3: min/max key: %s", err)
	}
	if !(kmin == 0 && kmax == B3_maxkey && okmin && okmax) {
		t.Fatalf("B3: min/max key wrong: got [%v, %v] (%v, %v);  want [%v, %v] (%v, %v)",
			kmin, kmax, okmin, okmax, 0, B3_maxkey, true, true)
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

	// XXX verify Entryv ?

	verifyFirstBucket(B3)
}
