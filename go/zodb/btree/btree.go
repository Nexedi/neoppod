// Copyright (C) 2018  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//

// Package btree provides B⁺ Trees for ZODB.
//
// It is modelled and data compatible with BTree/py package:
//
//	http://btrees.readthedocs.io
//	https://github.com/zopefoundation/BTrees
//
// A B⁺ tree consists of nodes. Only leaf tree nodes point to data.
// Intermediate tree nodes contains keys and pointer to next-level tree nodes.
// B⁺ tree always have uniform height - that is the path to any leaf node from
// the tree root is the same.	XXX
//
// BTree.Get(key) performs point-query.
//
// B⁺ Trees
package btree

//go:generate ./gen-btree IO int32 ziobtree.go
//go:generate ./gen-btree LO int64 zlobtree.go
