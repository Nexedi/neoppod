// Code generated by gen-btree; DO NOT EDIT.

// Copyright (c) 2001, 2002 Zope Foundation and Contributors.
// All Rights Reserved.
//
// Copyright (C) 2018-2019  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This software is subject to the provisions of the Zope Public License,
// Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
// THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
// WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
// FOR A PARTICULAR PURPOSE

package btree

// See https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/Development.txt#L198
// for LOBTree & LOBucket organization details.

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"

	pickle "github.com/kisielk/og-rek"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/internal/pickletools"
)

// LOBTree is a non-leaf node of a B⁺ tree.
//
// It contains []LOEntry in ↑ key order.
//
// It mimics LOBTree from btree/py.
type LOBTree struct {
	zodb.Persistent

	// https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeModuleTemplate.c#L205:
	//
	// firstbucket points to the bucket containing the smallest key in
	// the LOBTree.  This is found by traversing leftmost child pointers
	// (data[0].child) until reaching a LOBucket.
	firstbucket *LOBucket

	// https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeModuleTemplate.c#L211:
	//
	// The LOBTree points to 'len' children, via the "child" fields of the data
	// array.  There are len-1 keys in the 'key' fields, stored in increasing
	// order.  data[0].key is unused.  For i in 0 .. len-1, all keys reachable
	// from data[i].child are >= data[i].key and < data[i+1].key, at the
	// endpoints pretending that data[0].key is -∞ and data[len].key is +∞.
	data []LOEntry
}

// LOEntry is one LOBTree node entry.
//
// It contains key and child, who is either LOBTree or LOBucket.
//
// Key limits child's keys - see LOBTree.Entryv for details.
type LOEntry struct {
	key   int64
	child interface{} // LOBTree or LOBucket
}

// LOBucket is a leaf node of a B⁺ tree.
//
// It contains []LOBucketEntry in ↑ key order.
//
// It mimics LOBucket from btree/py.
type LOBucket struct {
	zodb.Persistent

	// https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeModuleTemplate.c#L179:
	//
	// A LOBucket wraps contiguous vectors of keys and values.  Keys are unique,
	// and stored in sorted order.  The 'values' pointer may be NULL if the
	// LOBucket is used to implement a set.  Buckets serving as leafs of BTrees
	// are chained together via 'next', so that the entire LOBTree contents
	// can be traversed in sorted order quickly and easily.

	next   *LOBucket       // the bucket with the next-larger keys
	keys   []int64         // 'len' keys, in increasing order
	values []interface{} // 'len' corresponding values
}

// LOBucketEntry is one LOBucket node entry.
//
// It contains key and value.
type LOBucketEntry struct {
	key   int64
	value interface{}
}

// ---- access []entry ----

// Key returns LOBTree entry key.
func (e *LOEntry) Key() int64 { return e.key }

// Child returns LOBTree entry child.
func (e *LOEntry) Child() interface{} { return e.child }

// Entryv returns entries of a LOBTree node.
//
// Entries keys limit the keys of all children reachable from an entry:
//
//	[i].Key ≤ [i].Child.*.Key < [i+1].Key		i ∈ [0, len([]))
//
//	[0].Key       = -∞	; always returned so
//	[len(ev)].Key = +∞	; should be assumed so
//
//
// Children of all entries are guaranteed to be of the same kind - either all LOBTree, or all LOBucket.
//
// The caller must not modify returned array.
func (t *LOBTree) Entryv() []LOEntry {
	return t.data
}

// Key returns LOBucket entry key.
func (e *LOBucketEntry) Key() int64 { return e.key }

// Value returns LOBucket entry value.
func (e *LOBucketEntry) Value() interface{} { return e.value }

// Entryv returns entries of a LOBucket node.
func (b *LOBucket) Entryv() []LOBucketEntry {
	ev := make([]LOBucketEntry, len(b.keys))
	for i, k := range b.keys {
		ev[i] = LOBucketEntry{k, b.values[i]}
	}
	return ev
}

// ---- node-level iteration ----

// FirstBucket returns bucket containing the smallest key in the tree.
func (t *LOBTree) FirstBucket() *LOBucket {
	return t.firstbucket
}

// Next returns tree bucket with next larger keys relative to current bucket.
func (b *LOBucket) Next() *LOBucket {
	return b.next
}

// ---- point query ----

// Get searches LOBTree by key.
//
// It loads intermediate LOBTree nodes from database on demand as needed.
//
// t need not be activated beforehand for Get to work.
func (t *LOBTree) Get(ctx context.Context, key int64) (_ interface{}, _ bool, err error) {
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
		case *LOBTree:
			t.PDeactivate()
			t = child
			err = t.PActivate(ctx)
			if err != nil {
				return nil, false, err
			}

		case *LOBucket:
			t.PDeactivate()
			return child.Get(ctx, key)
		}
	}
}

// Get searches LOBucket by key.
//
// TODO LOBucket.Get should not get ctx argument and just require that the bucket
// must be already activated. Correspondingly there should be no error returned.
func (b *LOBucket) Get(ctx context.Context, key int64) (_ interface{}, _ bool, err error) {
	defer xerr.Contextf(&err, "bucket(%s): get %v", b.POid(), key)
	err = b.PActivate(ctx)
	if err != nil {
		return nil, false, err
	}

	v, ok := b.get(key)
	b.PDeactivate()
	return v, ok, nil
}

// get searches LOBucket by key.
//
// no loading from database is done. The bucket must be already activated.
func (b *LOBucket) get(key int64) (interface{}, bool) {
	// search i: K(i-1) < k ≤ K(i)		; K(-1) = -∞, K(len) = +∞
	i := sort.Search(len(b.keys), func(i int) bool {
		return key <= b.keys[i]
	})

	if i == len(b.keys) || b.keys[i] != key {
		return nil, false // not found
	}
	return b.values[i], true
}

// ---- min/max key ----

// MinKey returns minimum key in LOBTree.
//
// If the tree is empty, ok=false is returned.
// The tree does not need to be activated beforehand.
func (t *LOBTree) MinKey(ctx context.Context) (_ int64, ok bool, err error) {
	defer xerr.Contextf(&err, "btree(%s): minkey", t.POid())
	err = t.PActivate(ctx)
	if err != nil {
		return 0, false, err
	}

	if len(t.data) == 0 {
		// empty btree
		t.PDeactivate()
		return 0, false, nil
	}

	// NOTE -> can also use t.firstbucket
	for {
		child := t.data[0].child.(zodb.IPersistent)
		t.PDeactivate()
		err = child.PActivate(ctx)
		if err != nil {
			return 0, false, err
		}

		switch child := child.(type) {
		case *LOBTree:
			t = child

		case *LOBucket:
			k, ok := child.MinKey()
			child.PDeactivate()
			return k, ok, nil
		}
	}
}

// MaxKey returns maximum key in LOBTree.
//
// If the tree is empty, ok=false is returned.
// The tree does not need to be activated beforehand.
func (t *LOBTree) MaxKey(ctx context.Context) (_ int64, _ bool, err error) {
	defer xerr.Contextf(&err, "btree(%s): maxkey", t.POid())
	err = t.PActivate(ctx)
	if err != nil {
		return 0, false, err
	}

	l := len(t.data)
	if l == 0 {
		// empty btree
		t.PDeactivate()
		return 0, false, nil
	}

	for {
		child := t.data[l-1].child.(zodb.IPersistent)
		t.PDeactivate()
		err = child.PActivate(ctx)
		if err != nil {
			return 0, false, err
		}

		switch child := child.(type) {
		case *LOBTree:
			t = child

		case *LOBucket:
			k, ok := child.MaxKey()
			child.PDeactivate()
			return k, ok, nil
		}
	}
}

// MinKey returns minimum key in LOBucket.
//
// If the bucket is empty, ok=false is returned.
func (b *LOBucket) MinKey() (_ int64, ok bool) {
	if len(b.keys) == 0 {
		return 0, false
	}
	return b.keys[0], true
}

// MaxKey returns maximum key in LOBucket.
//
// If the bucket is empty, ok=false is returned.
func (b *LOBucket) MaxKey() (_ int64, ok bool) {
	l := len(b.keys)
	if l == 0 {
		return 0, false
	}
	return b.keys[l-1], true
}

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

type lobucketState LOBucket // hide state methods from public API

// DropState implements zodb.Stateful to discard bucket state.
func (b *lobucketState) DropState() {
	b.next = nil
	b.keys = nil
	b.values = nil
}

// PySetState implements zodb.PyStateful to set bucket data from pystate.
func (b *lobucketState) PySetState(pystate interface{}) (err error) {
	t, ok := pystate.(pickle.Tuple)
	if !ok {
		return fmt.Errorf("top: expect (...); got %T", pystate)
	}
	if !(1 <= len(t) && len(t) <= 2) {
		return fmt.Errorf("top: expect [1..2](); got [%d]()", len(t))
	}

	// .next present
	if len(t) == 2 {
		next, ok := t[1].(*LOBucket)
		if !ok {
			return fmt.Errorf(".next must be LOBucket; got %T", t[1])
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
	b.keys = make([]int64, 0, n)
	b.values = make([]interface{}, 0, n)

	var kprev int64
	for i := 0; i < n; i++ {
		xk := t[2*i]
		v := t[2*i+1]

		k, ok := pickletools.Xint64(xk)
		if !ok {
			return fmt.Errorf("data: [%d]: key must be integer; got %T", i, xk)
		}

		kk := int64(k)
		if int64(kk) != k {
			return fmt.Errorf("data: [%d]: key overflows %T", i, kk)
		}

		if i > 0 && !(k > kprev) {
			return fmt.Errorf("data: [%d]: key not ↑", i)
		}
		kprev = k

		b.keys = append(b.keys, kk)
		b.values = append(b.values, v)
	}

	return nil
}


// https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeTemplate.c#L1087:
//
// For an empty LOBTree (self->len == 0), None.
//
// For a LOBTree with one child (self->len == 1), and that child is a bucket,
// and that bucket has a NULL oid, a one-tuple containing a one-tuple
// containing the bucket's state:
//
//     (
//         (
//              child[0].__getstate__(),
//         ),
//     )
//
// Else a two-tuple.  The first element is a tuple interleaving the LOBTree's
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

type lobtreeState LOBTree // hide state methods from public API

// DropState implements zodb.Stateful.
func (t *lobtreeState) DropState() {
	t.firstbucket = nil
	t.data = nil
}

// PySetState implements zodb.PyStateful to set btree data from pystate.
func (bt *lobtreeState) PySetState(pystate interface{}) (err error) {
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
		bucket := zodb.NewPersistent(reflect.TypeOf(LOBucket{}), bt.PJar()).(*LOBucket)
		err := (*lobucketState)(bucket).PySetState(t[0])
		if err != nil {
			return fmt.Errorf("bucket1: %s", err)
		}

		bt.firstbucket = bucket
		bt.data = []LOEntry{{key: 0, child: bucket}}
		return nil
	}

	// regular btree
	bt.firstbucket, ok = t[1].(*LOBucket)
	if !ok {
		return fmt.Errorf("first bucket: must be LOBucket; got %T", t[1])
	}

	t, ok = t[0].(pickle.Tuple)
	if !ok {
		return fmt.Errorf("data: expect (...); got %T", t[0])
	}
	if len(t)%2 == 0 {
		return fmt.Errorf("data: expect [!%%2](); got [%d]()", len(t))
	}

	n := (len(t) + 1) / 2
	bt.data = make([]LOEntry, 0, n)
	var kprev int64
	var childrenKind int // 1 - LOBTree, 2 - LOBucket
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

		kkey := int64(key)
		if int64(kkey) != key {
			return fmt.Errorf("data: [%d]: key overflows %T", i, kkey)
		}

		if i > 1 && !(key > kprev) {
			fmt.Errorf("data: [%d]: key not ↑", i)
		}
		kprev = key

		// check all children are of the same type
		var kind int // see childrenKind ^^^
		switch child.(type) {
		default:
			return fmt.Errorf("data: [%d]: child must be LOBTree|LOBucket; got %T", i, child)

		case *LOBTree:
			kind = 1
		case *LOBucket:
			kind = 2
		}

		if i == 0 {
			childrenKind = kind
		}
		if kind != childrenKind {
			fmt.Errorf("data: [%d]: children must be of the same type", i)
		}

		bt.data = append(bt.data, LOEntry{key: kkey, child: child})
	}

	return nil
}


// ---- register classes to ZODB ----

func init() {
	t := reflect.TypeOf
	zodb.RegisterClass("BTrees.LOBTree.LOBTree",  t(LOBTree{}),  t(lobtreeState{}))
	zodb.RegisterClass("BTrees.LOBTree.LOBucket", t(LOBucket{}), t(lobucketState{}))
}
