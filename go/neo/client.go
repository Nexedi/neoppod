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
	"fmt"
	"net/url"

	"../zodb"
)

type Client struct {
	storLink *NodeLink	// link to storage node
	storConn *Conn		// XXX main connection to storage
}

var _ zodb.IStorage = (*Client)(nil)

func (c *Client) StorageName() string {
	return "neo"	// TODO more specific
}

func (c *Client) Close() error {
	// NOTE this will abort all currently in-flght IO and close all connections over storLink
	err := c.storLink.Close()
	// XXX also wait for some goroutines to finish ?
	return err
}

func (c *Client) LastTid() (zodb.Tid, error) {
	// FIXME do not use global conn (see comment in openClientByURL)
	// XXX open new conn for this particular req/reply ?
	err := EncodeAndSend(c.storConn, &LastTransaction{})
	if err != nil {
		return 0, err	// XXX err context
	}

	reply, err := RecvAndDecode(c.storConn)
	if err != nil {
		// XXX err context (e.g. peer resetting connection -> currently only EOF)
		return 0, err
	}

	switch reply := reply.(type) {
	case *Error:
		return 0, reply	// XXX err context
	default:
		// XXX more error context ?
		return 0, fmt.Errorf("protocol error: unexpected reply: %T", reply)

	case *AnswerLastTransaction:
		return reply.Tid, nil
	}
}

func (c *Client) Load(xid zodb.Xid) (data []byte, tid zodb.Tid, err error) {
	// FIXME do not use global conn (see comment in openClientByURL)
	req := GetObject{Oid: xid.Oid}
	if xid.TidBefore {
		req.Serial = INVALID_TID
		req.Tid = xid.Tid
	} else {
		req.Serial = xid.Tid
		req.Tid = INVALID_TID
	}

	err = EncodeAndSend(c.storConn, &req)
	if err != nil {
		return nil, 0, err	// XXX err context
	}

	reply, err := RecvAndDecode(c.storConn)
	if err != nil {
		// XXX err context (e.g. peer resetting connection -> currently only EOF)
		return nil, 0, err
	}

	switch reply := reply.(type) {
	case *Error:
		return nil, 0, reply	// XXX err context
	default:
		// XXX more error context ?
		return nil, 0, fmt.Errorf("protocol error: unexpected reply: %T", reply)

	case *AnswerGetObject:
		data = reply.Data
		tid  = reply.Serial

		// TODO reply.Checksum - check sha1
		// TODO reply.Compression - decompress

		// reply.NextSerial
		// reply.DataSerial

		return data, tid, nil
	}
}

func (c *Client) Iterate(tidMin, tidMax zodb.Tid) zodb.IStorageIterator {
	// see notes in ../NOTES:"On iteration"
	panic("TODO")
}


// TODO read-only support
func openClientByURL(ctx context.Context, u *url.URL) (zodb.IStorage, error) {
	// XXX for now url is treated as storage node URL
	// XXX check/use other url fields
	storLink, err := Dial(ctx, "tcp", u.Host)
	if err != nil {
		return nil, err
	}

	// first identify ourselves via conn
	storType, err := IdentifyMe(storLink, CLIENT)
	if err != nil {
		return nil, err	// XXX err ctx
	}
	if storType != STORAGE {
		storLink.Close()	// XXX err
		return nil, fmt.Errorf("%v: peer is not storage (identifies as %v)", storLink, storType)
	}

	// identification passed

	// XXX only one conn is not appropriate for multiple goroutines/threads
	// asking storage in parallel. At the same time creating new conn for
	// every request is ok? -> not so good to create new goroutine per 1 object read
	// XXX -> server could reuse goroutines -> so not so bad ?
	conn, err := storLink.NewConn()
	if err != nil {
		storLink.Close()	// XXX err
		return nil, err	// XXX err ctx ?
	}

	return &Client{storLink, conn}, nil
}

//func Open(...) (*Client, error) {
//}

func init() {
	zodb.RegisterStorage("neo", openClientByURL)
}
