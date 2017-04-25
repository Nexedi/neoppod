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

package neo
// access to NEO database via ZODB interfaces

import (
	"context"
	"net/url"

	"../zodb"
)

type Client struct {
	storLink *NodeLink	// link to storage node
}

var _ zodb.IStorage = (*Client)(nil)

//func Open(...) (*Client, error) {
//}


func (c *Client) StorageName() string {
	return "neo"	// TODO more specific
}

func (c *Client) Close() error {
	panic("TODO")
}

func (c *Client) LastTid() zodb.Tid {
	panic("TODO")
}

func (c *Client) Load(xid zodb.Xid) (data []byte, tid zodb.Tid, err error) {
	panic("TODO")
}

func (c *Client) Iterate(tidMin, tidMax zodb.Tid) zodb.IStorageIterator {
	panic("TODO")
}


// TODO read-only support
func openClientByURL(u *url.URL) (zodb.IStorage, error) {
	// XXX for now url is treated as storage node URL
	// XXX check/use other url fields
	ctx := context.Background()	// XXX ok?
	storLink, err := Dial(ctx, "tcp", u.Host)
	if err != nil {
		return nil, err
	}

	return &Client{storLink}, nil
}

func init() {
	zodb.RegisterStorage("neo", openClientByURL)
}
