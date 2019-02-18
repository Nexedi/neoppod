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
// for IOBTree & IOBucket organization details.

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

// IOBTree is a non-leaf node of a B⁺ tree.
//
// It contains []IOEntry in ↑ key order.
//
// It mimics IOBTree from btree/py.
type IOBTree struct {
	zodb.Persistent

	// https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeModuleTemplate.c#L205:
	//
	// firstbucket points to the bucket containing the smallest key in
	// the IOBTree.  This is found by traversing leftmost child pointers
	// (data[0].child) until reaching a IOBucket.
	firstbucket *IOBucket

	// https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeModuleTemplate.c#L211:
	//
	// The IOBTree points to 'len' children, via the "child" fields of the data
	// array.  There are len-1 keys in the 'key' fields, stored in increasing
	// order.  data[0].key is unused.  For i in 0 .. len-1, all keys reachable
	// from data[i].child are >= data[i].key and < data[i+1].key, at the
	// endpoints pretending that data[0].key is -∞ and data[len].key is +∞.
	data []IOEntry
}

// IOEntry is one IOBTree node entry.
//
// It contains key and child, who is either IOBTree or IOBucket.
//
// Key limits child's keys - see IOBTree.Entryv for details.
type IOEntry struct {
	key   int32
	child zodb.IPersistent // IOBTree or IOBucket
}

// IOBucket is a leaf node of a B⁺ tree.
//
// It contains []IOBucketEntry in ↑ key order.
//
// It mimics IOBucket from btree/py.
type IOBucket struct {
	zodb.Persistent

	// https://github.com/zopefoundation/BTrees/blob/4.5.0-1-gc8bf24e/BTrees/BTreeModuleTemplate.c#L179:
	//
	// A IOBucket wraps contiguous vectors of keys and values.  Keys are unique,
	// and stored in sorted order.  The 'values' pointer may be NULL if the
	// IOBucket is used to implement a set.  Buckets serving as leafs of BTrees
	// are chained together via 'next', so that the entire IOBTree contents
	// can be traversed in sorted order quickly and easily.

	next   *IOBucket       // the bucket with the next-larger keys
	keys   []int32         // 'len' keys, in increasing order
	values []interface{} // 'len' corresponding values
}

// IOBucketEntry is one IOBucket node entry.
//
// It contains key and value.
type IOBucketEntry struct {
	key   int32
	value interface{}
}

// ---- access []entry ----

// Key returns IOBTree entry key.
func (e *IOEntry) Key() int32 { return e.key }

// Child returns IOBTree entry child.
func (e *IOEntry) Child() zodb.IPersistent { return e.child }

// Entryv returns entries of a IOBTree node.
//
// Entries keys limit the keys of all children reachable from an entry:
//
//	[i].Key ≤ [i].Child.*.Key < [i+1].Key		i ∈ [0, len([]))
//
//	[0].Key       = -∞	; always returned so
//	[len(ev)].Key = +∞	; should be assumed so
//
//
// Children of all entries are guaranteed to be of the same kind - either all IOBTree, or all IOBucket.
//
// The caller must not modify returned array.
func (t *IOBTree) Entryv() []IOEntry {
	return t.data
}

// Key returns IOBucket entry key.
func (e *IOBucketEntry) Key() int32 { return e.key }

// Value returns IOBucket entry value.
func (e *IOBucketEntry) Value() interface{} { return e.value }

// Entryv returns entries of a IOBucket node.
func (b *IOBucket) Entryv() []IOBucketEntry {
	ev := make([]IOBucketEntry, len(b.keys))
	for i, k := range b.keys {
		ev[i] = IOBucketEntry{k, b.values[i]}
	}
	return ev
}

// ---- leaf nodes iteration ----

// FirstBucket returns bucket containing the smallest key in the tree.
func (t *IOBTree) FirstBucket() *IOBucket {
	return t.firstbucket
}

// Next returns tree bucket with next larger keys relative to current bucket.
func (b *IOBucket) Next() *IOBucket {
	return b.next
}

// ---- point query ----

// Get searches IOBTree by key.
//
// It loads intermediate IOBTree nodes from database on demand as needed.
//
// t need not be activated beforehand for Get to work.
func (t *IOBTree) Get(ctx context.Context, key int32) (_ interface{}, _ bool, err error) {
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

		child := t.data[i].child
		t.PDeactivate()
		err = child.PActivate(ctx)
		if err != nil {
			return nil, false, err
		}

		switch child := child.(type) {
		case *IOBTree:
			t = child

		case *IOBucket:
			v, ok := child.Get(key)
			child.PDeactivate()
			return v, ok, nil
		}
	}
}

// Get searches IOBucket by key.
//
// no loading from database is done. The bucket must be already activated.
func (b *IOBucket) Get(key int32) (interface{}, bool) {
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

// MinKey returns minimum key in IOBTree.
//
// If the tree is empty, ok=false is returned.
// The tree does not need to be activated beforehand.
func (t *IOBTree) MinKey(ctx context.Context) (_ int32, ok bool, err error) {
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
		case *IOBTree:
			t = child

		case *IOBucket:
			k, ok := child.MinKey()
			child.PDeactivate()
			return k, ok, nil
		}
	}
}

// MaxKey returns maximum key in IOBTree.
//
// If the tree is empty, ok=false is returned.
// The tree does not need to be activated beforehand.
func (t *IOBTree) MaxKey(ctx context.Context) (_ int32, _ bool, err error) {
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
		case *IOBTree:
			t = child

		case *IOBucket:
			k, ok := child.MaxKey()
			child.PDeactivate()
			return k, ok, nil
		}
	}
}

// MinKey returns minimum key in IOBucket.
//
// If the bucket is empty, ok=false is returned.
func (b *IOBucket) MinKey() (_ int32, ok bool) {
	if len(b.keys) == 0 {
		return 0, false
	}
	return b.keys[0], true
}

// MaxKey returns maximum key in IOBucket.
//
// If the bucket is empty, ok=false is returned.
func (b *IOBucket) MaxKey() (_ int32, ok bool) {
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

type iobucketState IOBucket // hide state methods from public API

// DropState implements zodb.Stateful to discard bucket state.
func (b *iobucketState) DropState() {
	b.next = nil
	b.keys = nil
	b.values = nil
}

// PyGetState implements zodb.PyStateful to get bucket data as pystate.
func (b *iobucketState) PyGetState() interface{} {
	panic("TODO")
}

// PySetState implements zodb.PyStateful to set bucket data from pystate.
func (b *iobucketState) PySetState(pystate interface{}) (err error) {
	t, ok := pystate.(pickle.Tuple)
	if !ok {
		return fmt.Errorf("top: expect (...); got %T", pystate)
	}
	if !(1 <= len(t) && len(t) <= 2) {
		return fmt.Errorf("top: expect [1..2](); got [%d]()", len(t))
	}

	// .next present
	if len(t) == 2 {
		next, ok := t[1].(*IOBucket)
		if !ok {
			return fmt.Errorf(".next must be IOBucket; got %T", t[1])
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
	b.keys = make([]int32, 0, n)
	b.values = make([]interface{}, 0, n)

	var kprev int64
	for i := 0; i < n; i++ {
		xk := t[2*i]
		v := t[2*i+1]

		k, ok := pickletools.Xint64(xk)
		if !ok {
			return fmt.Errorf("data: [%d]: key must be integer; got %T", i, xk)
		}

		kk := int32(k)
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
// For an empty IOBTree (self->len == 0), None.
//
// For a IOBTree with one child (self->len == 1), and that child is a bucket,
// and that bucket has a NULL oid, a one-tuple containing a one-tuple
// containing the bucket's state:
//
//     (
//         (
//              child[0].__getstate__(),
//         ),
//     )
//
// Else a two-tuple.  The first element is a tuple interleaving the IOBTree's
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

type iobtreeState IOBTree // hide state methods from public API

// DropState implements zodb.Stateful.
func (t *iobtreeState) DropState() {
	t.firstbucket = nil
	t.data = nil
}

// PyGetState implements zodb.PyStateful to get btree data as pystate.
func (bt *iobtreeState) PyGetState() interface{} {
	panic("TODO")
}

// PySetState implements zodb.PyStateful to set btree data from pystate.
func (bt *iobtreeState) PySetState(pystate interface{}) (err error) {
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
		bucket := zodb.NewPersistent(reflect.TypeOf(IOBucket{}), bt.PJar()).(*IOBucket)
		err := (*iobucketState)(bucket).PySetState(t[0])
		if err != nil {
			return fmt.Errorf("bucket1: %s", err)
		}

		bt.firstbucket = bucket
		bt.data = []IOEntry{{key: 0, child: bucket}}
		return nil
	}

	// regular btree
	bt.firstbucket, ok = t[1].(*IOBucket)
	if !ok {
		return fmt.Errorf("first bucket: must be IOBucket; got %T", t[1])
	}

	t, ok = t[0].(pickle.Tuple)
	if !ok {
		return fmt.Errorf("data: expect (...); got %T", t[0])
	}
	if len(t)%2 == 0 {
		return fmt.Errorf("data: expect [!%%2](); got [%d]()", len(t))
	}

	n := (len(t) + 1) / 2
	bt.data = make([]IOEntry, 0, n)
	var kprev int64
	var childrenKind int // 1 - IOBTree, 2 - IOBucket
	for i, idx := 0, 0; i < n; i++ {
		key := int64(math.MinInt32) // int32(-∞)   (qualifies for ≤)
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

		kkey := int32(key)
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
			return fmt.Errorf("data: [%d]: child must be IOBTree|IOBucket; got %T", i, child)

		case *IOBTree:
			kind = 1
		case *IOBucket:
			kind = 2
		}

		if i == 0 {
			childrenKind = kind
		}
		if kind != childrenKind {
			fmt.Errorf("data: [%d]: children must be of the same type", i)
		}

		bt.data = append(bt.data, IOEntry{key: kkey, child: child.(zodb.IPersistent)})
	}

	return nil
}


// ---- register classes to ZODB ----

func init() {
	t := reflect.TypeOf
	zodb.RegisterClass("BTrees.IOBTree.IOBTree",  t(IOBTree{}),  t(iobtreeState{}))
	zodb.RegisterClass("BTrees.IOBTree.IOBucket", t(IOBucket{}), t(iobucketState{}))
}
