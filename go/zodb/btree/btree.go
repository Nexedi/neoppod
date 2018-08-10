// Copyright (c) 2001, 2002 Zope Foundation and Contributors.
// All Rights Reserved.
//
// Copyright (C) 2018  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This software is subject to the provisions of the Zope Public License,
// Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
// THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
// WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
// FOR A PARTICULAR PURPOSE

// Package btree provides B⁺ Trees for ZODB.
//
// It is modelled and data compatible with BTree/py package:
//
//	http://btrees.readthedocs.io
//	https://github.com/zopefoundation/BTrees
//
// A B⁺ tree consists of nodes. Only leaf tree nodes point to data.
// Intermediate tree nodes contains keys and pointer to next-level tree nodes.
// B⁺ always have uniform height - that is the path to any leaf node from the
// tree root is the same.
//
// BTree.Get(key) performs point-query.
//
// B⁺ Trees
package btree

// See https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/Development.txt#L198
// for BTree & Bucket organization details.

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"

	pickle "github.com/kisielk/og-rek"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

// KEY is the type for BTree keys.
//
// XXX -> template?
type KEY int64

// BTree is a non-leaf node of a B⁺ tree.
//
// It mimics ?OBTree from btree/py, with ? being any integer.
type BTree struct {
	zodb.Persistent

	// https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeModuleTemplate.c#L205:
	//
	// firstbucket points to the bucket containing the smallest key in
	// the BTree.  This is found by traversing leftmost child pointers
	// (data[0].child) until reaching a Bucket.
	firstbucket *Bucket

	// https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeModuleTemplate.c#L211:
	//
	// The BTree points to 'len' children, via the "child" fields of the data
	// array.  There are len-1 keys in the 'key' fields, stored in increasing
	// order.  data[0].key is unused.  For i in 0 .. len-1, all keys reachable
	// from data[i].child are >= data[i].key and < data[i+1].key, at the
	// endpoints pretending that data[0].key is -∞ and data[len].key is +∞.
	data []Entry
}

// Entry is one BTree node entry.
//
// It contains key and child, who is either BTree or Bucket.
//
// Key limits child's keys - see BTree.Entryv for details.
type Entry struct {
	key   KEY
	child interface{} // BTree or Bucket
}

// Bucket is a leaf node of a B⁺ tree.
//
// It mimics ?OBucket from btree/py, with ? being any integer.
type Bucket struct {
	zodb.Persistent

	// https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeModuleTemplate.c#L179:
	//
	// A Bucket wraps contiguous vectors of keys and values.  Keys are unique,
	// and stored in sorted order.  The 'values' pointer may be NULL if the
	// Bucket is used to implement a set.  Buckets serving as leafs of BTrees
	// are chained together via 'next', so that the entire BTree contents
	// can be traversed in sorted order quickly and easily.

	next   *Bucket       // the bucket with the next-larger keys
	keys   []KEY         // 'len' keys, in increasing order
	values []interface{} // 'len' corresponding values
}

// BucketEntry is one Bucket node entry.
//
// It contains key and value.
type BucketEntry struct {
	key   KEY
	value interface{}
}


// Key returns BTree entry key.
func (e *Entry) Key() KEY { return e.key }

// Child returns BTree entry child.
func (e *Entry) Child() interface{} { return e.child }

// Entryv returns entries of a BTree node.
//
// Entries keys limit the keys of all children reachable from an entry:
//
//	[i].Key ≤ [i].Child.*.Key < [i+1].Key		i ∈ [0, len([]))
//
//	[0].Key       = -∞	; always returned so
//	[len(ev)].Key = +∞	; should be assumed so
//
//
// Children of all entries are guaranteed to be of the same kind - either all BTree, or all Bucket.
//
// The caller must not modify returned array.
func (t *BTree) Entryv() []Entry {
	return t.data
}

// Key returns Bucket entry key.
func (e *BucketEntry) Key() KEY { return e.key }

// Value returns Bucket entry value.
func (e *BucketEntry) Value() interface{} { return e.value }

// Entryv returns entries of a Bucket node.
//
// XXX
func (b *Bucket) Entryv() []BucketEntry {
	ev := make([]BucketEntry, len(b.keys))
	for i, k := range b.keys {
		ev[i] = BucketEntry{k, b.values[i]}
	}
	return ev
}


// Get searches BTree by key.
//
// It loads intermediate BTree nodes from database on demand as needed.
//
// t need not be activated beforehand for Get to work.
func (t *BTree) Get(ctx context.Context, key KEY) (_ interface{}, _ bool, err error) {
	defer xerr.Contextf(&err, "btree(%s): get %v", t.POid(), key)
	err = t.PActivate(ctx)
	if err != nil {
		return nil, false, err
	}

	if len(t.data) == 0 {
		// empty btree
		t.PDeactivate()
		return nil, false, nil
	}

	for {
		// search i: K(i) ≤ k < K(i+1)	; K(0) = -∞, K(len) = +∞
		i := sort.Search(len(t.data), func(i int) bool {
			j := i + 1
			if j == len(t.data) {
				return true // [len].key = +∞
			}
			return key < t.data[j].key
		})

		switch child := t.data[i].child.(type) {
		case *BTree:
			t.PDeactivate()
			t = child
			err = t.PActivate(ctx)
			if err != nil {
				return nil, false, err
			}

		case *Bucket:
			t.PDeactivate()
			return child.Get(ctx, key)
		}
	}
}

// Get searches Bucket by key.
//
// TODO Bucket.Get should not get ctx argument and just require that the bucket
// must be already activated. Correspondingly there should be no error returned.
func (b *Bucket) Get(ctx context.Context, key KEY) (_ interface{}, _ bool, err error) {
	defer xerr.Contextf(&err, "bucket(%s): get %v", b.POid(), key)
	err = b.PActivate(ctx)
	if err != nil {
		return nil, false, err
	}

	v, ok := b.get(key)
	b.PDeactivate()
	return v, ok, nil
}

// get searches Bucket by key.
//
// no loading from database is done. The bucket must be already activated.
func (b *Bucket) get(key KEY) (interface{}, bool) {
	// search i: K(i-1) < k ≤ K(i)		; K(-1) = -∞, K(len) = +∞
	i := sort.Search(len(b.keys), func(i int) bool {
		return key <= b.keys[i]
	})

	if i == len(b.keys) || b.keys[i] != key {
		return nil, false // not found
	}
	return b.values[i], true
}

// TODO Bucket.MinKey
// TODO Bucket.MaxKey


// ---- serialization ----

// https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BucketTemplate.c#L1195:
//
// For a mapping bucket (self->values is not NULL), a one-tuple or two-tuple.
// The first element is a tuple interleaving keys and values, of length
// 2 * self->len.  The second element is the next bucket, present iff next is
// non-NULL:
//
//	(
//	     (keys[0], values[0], keys[1], values[1], ...,
//	                          keys[len-1], values[len-1]),
//	     <self->next iff non-NULL>
//	)

type bucketState Bucket // hide state methods from public API

// DropState implements Stateful to discard bucket state.
func (b *bucketState) DropState() {
	b.next = nil
	b.keys = nil
	b.values = nil
}

// PySetState implements PyStateful to set bucket data from pystate.
func (b *bucketState) PySetState(pystate interface{}) (err error) {
	t, ok := pystate.(pickle.Tuple)
	if !ok {
		return fmt.Errorf("top: expect (...); got %T", pystate)
	}
	if !(1 <= len(t) && len(t) <= 2) {
		return fmt.Errorf("top: expect [1..2](); got [%d]()", len(t))
	}

	// .next present
	if len(t) == 2 {
		next, ok := t[1].(*Bucket)
		if !ok {
			return fmt.Errorf(".next must be Bucket; got %T", t[1])
		}
		b.next = next
	}

	// main part
	t, ok = t[0].(pickle.Tuple)
	if !ok {
		return fmt.Errorf("data: expect (...); got %T", t[0])
	}
	if len(t)%2 != 0 {
		return fmt.Errorf("data: expect [%%2](); got [%d]()", len(t))
	}

	// reset arrays just in case
	n := len(t) / 2
	b.keys = make([]KEY, 0, n)
	b.values = make([]interface{}, 0, n)

	for i := 0; i < n; i++ {
		xk := t[2*i]
		v := t[2*i+1]

		k, ok := xk.(int64) // XXX use KEY	XXX -> Xint64
		if !ok {
			return fmt.Errorf("data: [%d]: key must be integer; got %T", i, xk)
		}

		// XXX check keys are sorted?
		b.keys = append(b.keys, KEY(k)) // XXX cast
		b.values = append(b.values, v)
	}

	return nil
}


// https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeTemplate.c#L1087:
//
// For an empty BTree (self->len == 0), None.
//
// For a BTree with one child (self->len == 1), and that child is a bucket,
// and that bucket has a NULL oid, a one-tuple containing a one-tuple
// containing the bucket's state:
//
//     (
//         (
//              child[0].__getstate__(),
//         ),
//     )
//
// Else a two-tuple.  The first element is a tuple interleaving the BTree's
// keys and direct children, of size 2*self->len - 1 (key[0] is unused and
// is not saved).  The second element is the firstbucket:
//
//     (
//          (child[0], key[1], child[1], key[2], child[2], ...,
//                                       key[len-1], child[len-1]),
//          self->firstbucket
//     )
//
// In the above, key[i] means self->data[i].key, and similarly for child[i].

type btreeState BTree // hide state methods from public API

// DropState implements zodb.Stateful.
func (t *btreeState) DropState() {
	t.firstbucket = nil
	t.data = nil
}

// PySetState implements zodb.PyStateful to set btree data from pystate.
func (bt *btreeState) PySetState(pystate interface{}) (err error) {
	// empty btree
	if _, ok := pystate.(pickle.None); ok {
		bt.firstbucket = nil
		bt.data = nil
		return nil
	}

	t, ok := pystate.(pickle.Tuple)
	if !ok {
		return fmt.Errorf("top: expect (...); got %T", pystate)
	}
	if !(1 <= len(t) && len(t) <= 2) {
		return fmt.Errorf("top: expect [1..2](); got [%d]()", len(t))
	}

	// btree with 1 child bucket without oid
	if len(t) == 1 {
		t, ok := t[0].(pickle.Tuple)
		if !ok {
			return fmt.Errorf("bucket1: expect [1](); got %T", t[0])
		}
		if len(t) != 1 {
			return fmt.Errorf("bucket1: expect [1](); got [%d]()", len(t))
		}
		bucket := zodb.NewPersistent(reflect.TypeOf(Bucket{}), bt.PJar()).(*Bucket)
		err := (*bucketState)(bucket).PySetState(t[0])
		if err != nil {
			return fmt.Errorf("bucket1: %s", err)
		}

		bt.firstbucket = bucket
		bt.data = []Entry{{key: 0, child: bucket}}
		return nil
	}

	// regular btree
	bt.firstbucket, ok = t[1].(*Bucket)
	if !ok {
		return fmt.Errorf("first bucket: must be Bucket; got %T", t[1])
	}

	t, ok = t[0].(pickle.Tuple)
	if !ok {
		return fmt.Errorf("data: expect (...); got %T", t[0])
	}
	if len(t)%2 == 0 {
		return fmt.Errorf("data: expect [!%%2](); got [%d]()", len(t))
	}

	n := (len(t) + 1) / 2
	bt.data = make([]Entry, 0, n)
	for i, idx := 0, 0; i < n; i++ {
		key := int64(math.MinInt64) // int64(-∞)   (qualifies for ≤)
		if i > 0 {
			// key[0] is unused and not saved
			key, ok = t[idx].(int64) // XXX Xint
			if !ok {
				return fmt.Errorf("data: [%d]: key must be integer; got %T", i, t[idx])
			}
			idx++
		}
		child := t[idx]
		idx++

		switch child.(type) {
		default:
			return fmt.Errorf("data: [%d]: child must be BTree|Bucket; got %T", i, child)

		// XXX check all children are of the same type
		case *BTree:  // ok
		case *Bucket: // ok
		}

		bt.data = append(bt.data, Entry{key: KEY(key), child: child})
	}

	return nil
}


// ---- register classes to ZODB ----

func init() {
	t := reflect.TypeOf
	zodb.RegisterClass("BTrees.LOBTree.LOBucket", t(Bucket{}), t(bucketState{}))
	zodb.RegisterClass("BTrees.LOBTree.LOBTree",  t(BTree{}),  t(btreeState{}))

	// XXX "I" means int32 in ZODB; we reuse int64 for now
	zodb.RegisterClass("BTrees.IOBTree.IOBucket", t(Bucket{}), t(bucketState{}))
	zodb.RegisterClass("BTrees.IOBTree.IOBTree",  t(BTree{}),  t(btreeState{}))
}
