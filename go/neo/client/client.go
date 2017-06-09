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

// Package client provides ZODB interface for accessing NEO cluster
package client

import (
	"context"
	"net/url"

	"../../neo"
	"../../zodb"
)

// Client talks to NEO cluster and exposes access it via ZODB interfaces
type Client struct {
	// XXX move -> nodeCommon?
	// ---- 8< ----
	myInfo		neo.NodeInfo	// XXX -> only NodeUUID
	clusterName	string

	net		neo.Network	// network we are sending/receiving on
	masterAddr	string		// address of master	XXX -> Address ?
	// ---- 8< ----


	storLink *neo.NodeLink	// link to storage node
	storConn *neo.Conn	// XXX main connection to storage
}

var _ zodb.IStorage = (*Client)(nil)

func (c *Client) StorageName() string {
	return "neo"	// TODO more specific (+ cluster name, ...)
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
	reply := neo.AnswerLastTransaction{}
	err := c.storConn.Ask(&neo.LastTransaction{}, &reply)
	if err != nil {
		return 0, err	// XXX err ctx
	}
	return reply.Tid, nil
}

func (c *Client) Load(xid zodb.Xid) (data []byte, tid zodb.Tid, err error) {
	// FIXME do not use global conn (see comment in openClientByURL)
	req := neo.GetObject{Oid: xid.Oid}
	if xid.TidBefore {
		req.Serial = neo.INVALID_TID
		req.Tid = xid.Tid
	} else {
		req.Serial = xid.Tid
		req.Tid = neo.INVALID_TID
	}

	resp := neo.AnswerGetObject{}
	err = c.storConn.Ask(&req, &resp)
	if err != nil {
		return nil, 0, err	// XXX err context
	}

	// TODO reply.Checksum - check sha1
	// TODO reply.Compression - decompress

	// reply.NextSerial
	// reply.DataSerial
	return resp.Data, resp.Serial, nil
}

func (c *Client) Iterate(tidMin, tidMax zodb.Tid) zodb.IStorageIterator {
	// see notes in ../NOTES:"On iteration"
	panic("TODO")
}


// NewClient creates and identifies new client connected to storage over storLink
func NewClient(storLink *neo.NodeLink) (*Client, error) {
	// TODO .myInfo.NodeType = CLIENT
	// .clusterName = clusterName
	// .net = ...
	cli := &Client{}
	//return &Client{storLink, storConn}, nil

	// XXX move -> Run?
	// first identify ourselves to peer
	accept, err := neo.IdentifyWith(neo.STORAGE, storLink, cli.myInfo, cli.clusterName)
	if err != nil {
		return nil, err
	}

	// TODO verify accept more
	_ = accept

	// identification passed

	// XXX only one conn is not appropriate for multiple goroutines/threads
	// asking storage in parallel. At the same time creating new conn for
	// every request is ok? -> not so good to create new goroutine per 1 object read
	// XXX -> server could reuse goroutines -> so not so bad ?
	storConn, err := storLink.NewConn()
	if err != nil {
		return nil, err	// XXX err ctx
	}

	_ = storConn	// XXX temp
	return cli, nil
}

// TODO read-only support
func openClientByURL(ctx context.Context, u *url.URL) (zodb.IStorage, error) {
	// XXX for now url is treated as storage node URL
	// XXX check/use other url fields
	net := neo.NetPlain("tcp")	// TODO + TLS; not only "tcp" ?
	storLink, err := neo.Dial(ctx, net, u.Host)
	if err != nil {
		return nil, err
	}

	// close storLink on error or ctx cancel
	defer func() {
		if err != nil {
			storLink.Close()
		}
	}()


	// XXX try to prettify this
	type Result struct {*Client; error}
	done := make(chan Result, 1)
	go func() {
		client, err := NewClient(storLink)
		done <- Result{client, err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case r := <-done:
		return r.Client, r.error
	}

}

//func Open(...) (*Client, error) {
//}

func init() {
	zodb.RegisterStorage("neo", openClientByURL)
}
