// Copyright (C) 2018  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//

// Package btree provides B⁺ Trees for ZODB.
//
// It is modelled and data compatible with BTree/py package:
//
//	http://btrees.readthedocs.io
//	https://github.com/zopefoundation/BTrees
package btree

//go:generate ./gen-btree IO int32 ziobtree.go
//go:generate ./gen-btree LO int64 zlobtree.go
