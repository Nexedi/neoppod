// Copyright (C) 2018-2019  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>

// Package btree provides B⁺ Trees for ZODB.
//
// It is modelled and data compatible with BTree/py package:
//
//	http://btrees.readthedocs.io
//	https://github.com/zopefoundation/BTrees
//
// A B⁺ tree consists of nodes. Only leaf tree nodes point to data.
// Intermediate tree nodes contains keys and pointer to next-level tree nodes.
//
// A well-balanced B⁺ tree always have uniform depth - that is the path to any
// leaf node from the tree root is the same. However historically ZODB/py does
// not always balance B⁺ trees well(*), so this property is not preserved.
// Nevertheless an intermediate B⁺ tree node always has children of the same
// kind: they are either all leafs or all intermediate nodes(+).
//
// BTree and Bucket represent an intermediate and a leaf tree node correspondingly.
// Node represents any of them.
//
// node.Get(key) performs point-query.
//
// node.{Min,Max}Key() returns key-range limit for all children/values under the node.
//
// node.Entryv() returns [] of (key, child/value).
//
// BTree.FirstBucket() and Bucket.Next() allow to iterate through leaf B⁺ tree nodes.
//
// BTree.V<op>(..., visit) performs <op> with calling visit on every accessed tree node.
//
// --------
//
// (*) https://github.com/zopefoundation/ZODB/blob/3.10.7-4-gb8d7a8567/src/BTrees/Development.txt#L211
//
// (+) https://github.com/zopefoundation/ZODB/blob/3.10.7-4-gb8d7a8567/src/BTrees/Development.txt#L231
package btree

//go:generate ./gen-btree IO int32 ziobtree.go
//go:generate ./gen-btree LO int64 zlobtree.go

import (
	"fmt"
	"reflect"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/internal/pickletools"
)

// Length is equivalent of BTrees.Length.Length in BTree/py.
type Length struct {
	zodb.Persistent

	value int
}

type lengthState Length // hide state methods from public API

// DropState implements zodb.Stateful.
func (l *lengthState) DropState() {
	l.value = 0
}

// PyGetState implements zodb.PyStateful.
func (l *lengthState) PyGetState() interface{} {
	return l.value
}

// PySetState implements zodb.PyStateful.
func (l *lengthState) PySetState(pystate interface{}) (err error) {
	v, ok := pickletools.Xint64(pystate)
	if !ok {
		return fmt.Errorf("state must be int; got %T", pystate)
	}

	l.value = int(v)
	return nil
}

// ---- register classes to ZODB ----

func init() {
	t := reflect.TypeOf
	zodb.RegisterClass("BTrees.Length.Length", t(Length{}),  t(lengthState{}))
}
