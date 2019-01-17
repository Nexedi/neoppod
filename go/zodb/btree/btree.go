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
//
// node.Get(key) performs point-query.
//
// node.{Min,Max}Key() returns key-range limit for all children/values under the node.
//
// node.Entryv() returns [] of (key, child/value).
//
// BTree.FirstBucket() and Bucket.Next() allow to iterate through leaf B⁺ tree nodes.
//
// --------
//
// (*) https://github.com/zopefoundation/ZODB/blob/3.10.7-4-gb8d7a8567/src/BTrees/Development.txt#L211
//
// (+) https://github.com/zopefoundation/ZODB/blob/3.10.7-4-gb8d7a8567/src/BTrees/Development.txt#L231
package btree

//go:generate ./gen-btree IO int32 ziobtree.go
//go:generate ./gen-btree LO int64 zlobtree.go
