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

// Package btree provides support for working with ZODB BTrees.
//
// XXX
//
// XXX name -> zbtree ?
package btree

import (
	"context"
	"sort"

	"lab.nexedi.com/kirr/neo/go/zodb"
)

// XXX temp
type KEY int64


// Bucket mimics ?OBucket from btree/py, with ? being any integer.
//
// original py description:
//
// A Bucket wraps contiguous vectors of keys and values.  Keys are unique,
// and stored in sorted order.  The 'values' pointer may be NULL if the
// Bucket is used to implement a set.  Buckets serving as leafs of BTrees
// are chained together via 'next', so that the entire BTree contents
// can be traversed in sorted order quickly and easily.
type Bucket struct {
	pyobj *zodb.PyObject

	next   *Bucket		// the bucket with the next-larger keys
	keys   []KEY		// 'len' keys, in increasing order
	values []interface{}	// 'len' corresponding values		XXX merge k/v ?
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
	pyobj *zodb.PyObject

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
// XXX ok -> ErrKeyNotFound?
func (t *BTree) Get(ctx context.Context, key KEY) (interface{}, bool, error) {
	t.PActivate(ctx)	// XXX err

	if len(t.data) == 0 {
		// empty btree
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
			// XXX t.PAllowDeactivate
			t = child
			t.PActivate(ctx)	// XXX err

		case *Bucket:
			child.PActivate(ctx)	// XXX err
			x, ok := child.get(key)
			return x, ok, nil
		}
	}
}

// get searches Bucket by key.
//
// no loading from database is done. The bucket must be already activated.
func (b *Bucket) get(key KEY) (interface{}, bool) {
	// XXX b.PActivate ?	XXX better caller? (or inside if get -> Get)

	// search i: K(i-1) < k ≤ K(i)		; K(-1) = -∞, K(len) = +∞
	i := sort.Search(len(b.keys), func(i int) bool {
		return key <= b.keys[i]
	})

	if i == len(b.keys) || b.keys[i] != key {
		return nil, false // not found
	}
	return b.values[i], true
}


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


// XXX Bucket.MinKey ?
// XXX Bucket.MaxKey ?


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
