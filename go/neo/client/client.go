// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

// Package client provides ZODB interface for accessing NEO cluster.
package client

import (
	"context"
	"math/rand"
	"net/url"

	"lab.nexedi.com/kirr/neo/go/neo"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
)

// Client talks to NEO cluster and exposes access to it via ZODB interfaces
type Client struct {
	node neo.NodeCommon

//	storLink *neo.NodeLink	// link to storage node
//	storConn *neo.Conn	// XXX main connection to storage
}

var _ zodb.IStorage = (*Client)(nil)

func (c *Client) StorageName() string {
	return "neo"
}

// NewClient creates new client node.
// it will connect to master @masterAddr and identify with sepcified cluster name
func NewClient(clusterName, masterAddr string, net xnet.Networker) (*Client, error) {
	cli := &Client{
		node: neo.NodeCommon{
			MyInfo:		neo.NodeInfo{Type: neo.CLIENT, Addr: neo.Address{}},
			ClusterName:	clusterName,
			Net:		net,
			MasterAddr:	masterAddr,

			//NodeTab:	&neo.NodeTable{},
			//PartTab:	&neo.PartitionTable{},
		},
	}

	// XXX -> talkMaster
	cli.node.Dial(context.TODO(), neo.MASTER, masterAddr)
	panic("TODO")
}


func (c *Client) Close() error {
	panic("TODO")
//	// NOTE this will abort all currently in-flght IO and close all connections over storLink
//	err := c.storLink.Close()
//	// XXX also wait for some goroutines to finish ?
//	return err
}

func (c *Client) LastTid() (zodb.Tid, error) {
	panic("TODO")
/*
	c.Mlink // XXX check we are connected
	conn, err := c.Mlink.NewConn()
	if err != nil {
		// XXX
	}
	// XXX defer conn.Close

	// FIXME do not use global conn (see comment in openClientByURL)
	// XXX open new conn for this particular req/reply ?
	reply := neo.AnswerLastTransaction{}
	err := conn.Ask(&neo.LastTransaction{}, &reply)
	if err != nil {
		return 0, err	// XXX err ctx
	}
	return reply.Tid, nil
*/
}

func (c *Client) LastOid() (zodb.Oid, error) {
	// XXX there is no LastOid in NEO/py
	panic("TODO")
}

func (c *Client) Load(xid zodb.Xid) (data []byte, tid zodb.Tid, err error) {
	// XXX check pt is operational first? -> no if there is no data - we'll
	// just won't find ready cell
	cellv := c.node.PartTab.Get(xid.Oid)
	// XXX cellv = filter(cellv, UP_TO_DATE)
	cell := cellv[rand.Intn(len(cellv))]
	stor := c.node.NodeTab.Get(cell.NodeUUID)
	if stor == nil {
		panic(0) // XXX
	}
	// XXX check stor.State == RUNNING

	Sconn, err := stor.Conn()
	if err != nil {
		panic(0) // XXX
	}

	req := neo.GetObject{Oid: xid.Oid}
	if xid.TidBefore {
		req.Serial = neo.INVALID_TID
		req.Tid = xid.Tid
	} else {
		req.Serial = xid.Tid
		req.Tid = neo.INVALID_TID
	}

	resp := neo.AnswerGetObject{}
	err = Sconn.Ask(&req, &resp)
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


// TODO read-only support
func openClientByURL(ctx context.Context, u *url.URL) (zodb.IStorage, error) {
	// XXX u.Host -> masterAddr (not storage)
	panic("TODO")

/*
	// XXX check/use other url fields
	net := xnet.NetPlain("tcp")	// TODO + TLS; not only "tcp" ?
	storLink, err := neo.DialLink(ctx, net, u.Host)		// XXX -> Dial
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
*/
}

func init() {
	zodb.RegisterStorage("neo", openClientByURL)
}
