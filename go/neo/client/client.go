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
	"crypto/sha1"
	"fmt"
	"math/rand"
	"net/url"
	"time"

	"lab.nexedi.com/kirr/neo/go/neo"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/xcommon/log"
	"lab.nexedi.com/kirr/neo/go/xcommon/task"
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
func NewClient(clusterName, masterAddr string, net xnet.Networker) *Client {
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

	// spawn background process which performs master talk
	go cli.talkMaster(context.TODO())	// XXX ctx = "client(?)"

	return cli
}


func (c *Client) Close() error {
	panic("TODO")
//	// NOTE this will abort all currently in-flght IO and close all connections over storLink
//	err := c.storLink.Close()
//	// XXX also wait for some goroutines to finish ?
//	return err
}

// --- connection with master ---

// talkMaster connects to master, announces self and receives notifications.
// it tries to persist master link reconnecting as needed.
//
// XXX C -> M for commit
//
// XXX always error  (dup Storage.talkMaster) ?
func (c *Client) talkMaster(ctx context.Context) (err error) {
	defer task.Runningf(&ctx, "talk master(%v)", c.node.MasterAddr)(&err)

	// XXX dup wrt Storage.talkMaster
	for {
		err := c.talkMaster1(ctx)
		log.Error(ctx, err)

		// TODO if err = shutdown -> return

		// exit on cancel / throttle reconnecting 
		select {
		case <-ctx.Done():
			return ctx.Err()

		// XXX 1s hardcoded -> move out of here
		case <-time.After(1*time.Second):
			// ok
		}
	}
}

func (c *Client) talkMaster1(ctx context.Context) (err error) {
	// XXX dup from Server.talkMaster1
	// XXX put logging into Dial?
	log.Info(ctx, "connecting ...")
	Mconn, accept, err := stor.node.Dial(ctx, neo.MASTER, stor.node.MasterAddr)
	if err != nil {
		// FIXME it is not only identification - e.g. ECONNREFUSED
		log.Info(ctx, "identification rejected")	// XXX ok here? (err is logged above)
		return err
	}

	log.Info(ctx, "identification accepted")
	Mlink := Mconn.Link()

	defer xio.CloseWhenDone(ctx, Mlink)()

	// XXX .nodeTab.Reset()

	Ask(partiotionTable)
	Ask(lastTransaction)

	for {
		msg, err := Mconn.Recv()
		if err != nil {
			return err
		}

		switch msg.(type) {
		default:
			return fmt.Errorf("unexpected message: %T", msg)

		case *neo.NotifyPartitionTable:
			// TODO M sends whole PT

		//case *neo.NotifyPartitionChanges:
		//	// TODO M sends δPT

		case *neo.NotifyNodeInformation:
			// TODO

		case *neo.NotifyClusterState:
			// TODO

	}
}

// --- user API calls ---

func (c *Client) LastTid(ctx context.Context) (zodb.Tid, error) {
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

func (c *Client) LastOid(ctx context.Context) (zodb.Oid, error) {
	// XXX there is no LastOid in NEO/py
	panic("TODO")
}

func (c *Client) Load(ctx context.Context, xid zodb.Xid) (data []byte, serial zodb.Tid, err error) {
	// XXX check pt is operational first? -> no if there is no data - we'll
	// just won't find ready cell
	//
	// XXX or better still check first M told us ok to go? (ClusterState=RUNNING)
	//if c.node.ClusterState != ClusterRunning {
	//	return nil, 0, &Error{NOT_READY, "cluster not operational"}
	//}

	cellv := c.node.PartTab.Get(xid.Oid)
	// XXX cellv = filter(cellv, UP_TO_DATE)
	if len(cellv) == 0 {
		return nil, 0, fmt.Errorf("no storages alive for oid %v", xid.Oid)	// XXX err ctx
	}
	cell := cellv[rand.Intn(len(cellv))]
	stor := c.node.NodeTab.Get(cell.NodeUUID)
	if stor == nil {
		return nil, 0, fmt.Errorf("storage %v not yet known", cell.NodeUUID)	// XXX err ctx
	}
	// XXX check stor.State == RUNNING -> in link

	Sconn := stor.Conn // XXX temp stub
	//Sconn, err := stor.Conn()
	if err != nil {
		return nil, 0, err	// XXX err ctx
	}
	defer lclose(ctx, Sconn)

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

	checksum := sha1.Sum(data)
	if checksum != resp.Checksum {
		// XXX data corrupt
	}

	data = resp.Data
	if resp.Compression {
		data, err = decompress(resp.Data, make([]byte, 0, len(resp.Data)))
		if err != nil {
			// XXX data corrupt
		}
	}

	// reply.NextSerial
	// reply.DataSerial
	return data, resp.Serial, nil
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
