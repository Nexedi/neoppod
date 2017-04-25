// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Open Source Initiative approved licenses and Convey
// the resulting work. Corresponding source of such a combination shall include
// the source code for all other software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

// Package client provides access to NEO database via ZODB interfaces
package client

import (

	"../../neo"
	"../../zodb"
)

type NEOClient struct {
	storLink neo.NodeLink	// link to storage node
}

var _ zodb.IStorage = (*NEOClient)(nil)

//func Open(...) (*NEOClient, error) {
//}


func (c *NEOClient) StorageName() string {
	return "neo"	// TODO more specific
}

func (c *NEOClient) Close() error {
	panic("TODO")
}

func (c *NEOClient) LastTid() zodb.Tid {
	panic("TODO")
}

func (c *NEOClient) Load(xid zodb.Xid) (data []byte, tid zodb.Tid, err error) {
	panic("TODO")
}

func (c *NEOClient) Iterate(tidMin, tidMax zodb.Tid) zodb.IStorageIterator {
	panic("TODO")
}
