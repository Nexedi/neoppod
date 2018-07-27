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
//	https://github.com/zopefoundation/BTrees
package btree

import (
	"context"
	"sort"

	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/zodb"
	pickle "github.com/kisielk/og-rek"
)

// XXX -> template
type KEY int64


// Bucket mimics ?OBucket from btree/py, with ? being any integer.
//
// py description:
//
// A Bucket wraps contiguous vectors of keys and values.  Keys are unique,
// and stored in sorted order.  The 'values' pointer may be NULL if the
// Bucket is used to implement a set.  Buckets serving as leafs of BTrees
// are chained together via 'next', so that the entire BTree contents
// can be traversed in sorted order quickly and easily.
type Bucket struct {
	*zodb.PyPersistent

	next   *Bucket		// the bucket with the next-larger keys
	keys   []KEY		// 'len' keys, in increasing order
	values []interface{}	// 'len' corresponding values
}

// zBTreeItem mimics BTreeItem from btree/py.
type zBTreeItem struct {
	key	KEY
	child	interface{}	// BTree or Bucket
}

// BTree mimics ?OBTree from btree/py, with ? being any integer.
//
// See https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/Development.txt#L198
// for details.
type BTree struct {
	*zodb.PyPersistent

	// firstbucket points to the bucket containing the smallest key in
	// the BTree.  This is found by traversing leftmost child pointers
	// (data[0].child) until reaching a Bucket.
	firstbucket *Bucket

	// The BTree points to 'len' children, via the "child" fields of the data
	// array.  There are len-1 keys in the 'key' fields, stored in increasing
	// order.  data[0].key is unused.  For i in 0 .. len-1, all keys reachable
	// from data[i].child are >= data[i].key and < data[i+1].key, at the
	// endpoints pretending that data[0].key is minus infinity and
	// data[len].key is positive infinity.
	data	[]zBTreeItem
}

// Get searches BTree by key.
//
// It loads intermediate BTree nodes from database on demand as needed.
func (t *BTree) Get(ctx context.Context, key KEY) (_ interface{}, _ bool, err error) {
	defer xerr.Contextf(&err, "btree(%s): get %s", t.POid(), key)	// XXX + url?
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
				return true	// [len].key = +∞
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
func (b *Bucket) Get(ctx context.Context, key KEY) (_ interface{}, _ bool, err error) {
	defer xerr.Contextf(&err, "bucket(%s): get %s", b.POid(), key)	// XXX + url?
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

// XXX Bucket.MinKey ?
// XXX Bucket.MaxKey ?


// ---- serialization ----

// from https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeTemplate.c#L1087:
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

// DropState implements zodb.Stateful.
func (t *BTree) DropState() {
	t.firstbucket = nil
	t.data = nil
}

// PySetState implements zodb.PyStateful to set btree data from pystate.
func (bt *BTree) PySetState(pystate interface{}) error {
	// empty btree
	if _, ok := pystate.(pickle.None); ok {
		bt.firstbucket = nil
		bt.data = nil
		return nil
	}

	t, ok := pystate.(pickle.Tuple)
	if !ok || !(1 <= len(t) && len(t) <= 2) {
		// XXX
	}

	// btree with 1 child bucket without oid
	if len(t) == 1 {
		bucket := &Bucket{PyPersistent: nil /* FIXME */}
		err := bucket.PySetState(t[0])
		if err != nil {
			// XXX
		}

		bt.firstbucket = bucket
		bt.data = []zBTreeItem{{key: 0, child: bucket}}
		return nil
	}

	// regular btree
	t, ok = t[0].(pickle.Tuple)
	if !(ok && len(t) % 2 == 0) {
		// XXX
	}

	bt.firstbucket, ok = t[1].(*Bucket)
	if !ok {
		// XXX
	}

	n := len(t) / 2
	bt.data = make([]zBTreeItem, 0, n)
	for i, idx := 0, 0; i < n; i++ {
		key := int64(0)
		if i > 0 {
			// key[0] is unused and not saved
			key, ok = t[idx].(int64)	// XXX Xint
			if ! ok {
				// XXX
			}
			idx++
		}
		child := t[idx]
		idx++

		switch child.(type) {
		default:
			// XXX

		case *BTree:  // ok
		case *Bucket: // ok
		}

		bt.data = append(bt.data, zBTreeItem{key: KEY(key), child: child})
	}

	return nil
}


// from https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BucketTemplate.c#L1195:
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

// DropState implements Stateful to discard bucket state.
func (b *Bucket) DropState() {
	b.next = nil
	b.keys = nil
	b.values = nil
}

// PySetState implements PyStateful to set bucket data from pystate.
func (b *Bucket) PySetState(pystate interface{}) error {
	t, ok := pystate.(pickle.Tuple)
	if !ok || !(1 <= len(t) && len(t) <= 2) {
		// XXX complain
	}

	// .next present
	if len(t) == 2 {
		next, ok := t[1].(*Bucket)
		if !ok {
			// XXX
		}
		b.next = next
	}

	// main part
	t, ok = t[0].(pickle.Tuple)
	// XXX if !ok || (len(t) % 2 != 0)

	// reset arrays just in case
	n := len(t) / 2
	b.keys = make([]KEY, 0, n)
	b.values = make([]interface{}, 0, n)

	for i := 0; i < n; i++ {
		xk := t[2*i]
		v := t[2*i+1]

		k, ok := xk.(int64)	// XXX use KEY	XXX -> Xint64
		if !ok {
			// XXX
		}

		// XXX check keys are sorted?
		b.keys = append(b.keys, KEY(k))		// XXX cast
		b.values = append(b.values, v)
	}

	return nil
}


// ---- register classes to ZODB ----

func bucketNew(base *zodb.Persistent) zodb.IPersistent {
	// XXX simplify vvv
	return &Bucket{PyPersistent: &zodb.PyPersistent{Persistent: base}}
}

func btreeNew(base *zodb.Persistent) zodb.IPersistent {
	// XXX simplify vvv
	return &BTree{PyPersistent: &zodb.PyPersistent{Persistent: base}}
}

func init() {
	zodb.RegisterClass("zodb.BTree.LOBucket", bucketNew)
	zodb.RegisterClass("zodb.BTree.LOBtree",  btreeNew)
}
