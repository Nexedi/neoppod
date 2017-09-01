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

// Package client provides ZODB storage interface for accessing NEO cluster.
package client

import (
	"context"
	"crypto/sha1"
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"lab.nexedi.com/kirr/go123/xerr"

	"lab.nexedi.com/kirr/neo/go/neo"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/xcommon/log"
	"lab.nexedi.com/kirr/neo/go/xcommon/task"
	"lab.nexedi.com/kirr/neo/go/xcommon/xio"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
)

// Client talks to NEO cluster and exposes access to it via ZODB interfaces.
type Client struct {
	node *neo.NodeApp

	talkMasterCancel func()

	// link to master - established and maintained by talkMaster.
	// users retrieve it via masterLink.
	mlinkMu    sync.Mutex
	mlink      *neo.NodeLink
	mlinkReady chan struct{} // reinitialized at each new talk cycle

	// operational state in node is maintained by recvMaster.
	// users retrieve it via withOperational.
	//
	// NOTE being operational means:
	// - link to master established and is ok
	// - .PartTab is operational wrt .NodeTab
	// - .ClusterState = RUNNING	<- XXX needed?
	//
	// however master link is accessed separately (see ^^^ and masterLink)
	operational bool // XXX <- somehow move to NodeApp?
	opReady	    chan struct{} // reinitialized each time state becomes non-operational
}

var _ zodb.IStorage = (*Client)(nil)

func (c *Client) StorageName() string {
	return "neo"
}

// NewClient creates new client node.
//
// It will connect to master @masterAddr and identify with sepcified cluster name.
func NewClient(clusterName, masterAddr string, net xnet.Networker) *Client {
	cli := &Client{
		node:       neo.NewNodeApp(net, neo.CLIENT, clusterName, masterAddr, ""),
		mlinkReady: make(chan struct{}),
	}

	// spawn background process which performs master talk
	ctx, cancel := context.WithCancel(context.Background())
	cli.talkMasterCancel = cancel
	go cli.talkMaster(ctx)

	return cli
}


func (c *Client) Close() error {
	c.talkMasterCancel()
	// XXX wait talkMaster finishes
	// XXX what else?
	return nil
}

// --- connection with master ---

// masterLink returns link to primary master.
//
// NOTE that even if masterLink returns != nil, the master link can become
// non-operational at any later time. (such cases will be reported as
// ErrLinkDown returned by all mlink operations)
func (c *Client) masterLink(ctx context.Context) (*neo.NodeLink, error) {
	for {
		c.mlinkMu.Lock()
		mlink := c.mlink
		ready := c.mlinkReady
		c.mlinkMu.Unlock()

		if mlink != nil {
			return mlink, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-ready:
			// ok - try to relock mlinkMu and read mlink again.
		}
	}
}

// withOperational waits for cluster state to be operational.
//
// If successful it returns with operational state RLocked (c.node.StateMu) and
// unlocked otherwise.
//
// The only error possible is if provided ctx cancel.
func (c *Client) withOperational(ctx context.Context) error {
	for {
		c.node.StateMu.RLock()
		if c.operational {
			return nil
		}

		ready := c.opReady
		c.node.StateMu.RUnlock()

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ready:
			// ok - try to relock and read again.
		}
	}
}

// talkMaster connects to master, announces self and receives notifications.
// it tries to persist master link reconnecting as needed.
//
// XXX C -> M for commit
//
// XXX always error  (dup Storage.talkMaster) ?
func (c *Client) talkMaster(ctx context.Context) (err error) {
	defer task.Runningf(&ctx, "client: talk master(%v)", c.node.MasterAddr)(&err)

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
	mlink, accept, err := c.node.Dial(ctx, neo.MASTER, c.node.MasterAddr)
	if err != nil {
		// FIXME it is not only identification - e.g. ECONNREFUSED
		return err
	}

	// XXX vvv dup from Server.talkMaster1 ?

	// XXX -> node.Dial ?
	if accept.YourUUID != c.node.MyInfo.UUID {
		log.Infof(ctx, "master told us to have uuid=%v", accept.YourUUID)
		c.node.MyInfo.UUID = accept.YourUUID
	}

	// set c.mlink and notify waiters
	c.mlinkMu.Lock()
	c.mlink = mlink
	ready := c.mlinkReady
	c.mlinkReady = make(chan struct{})
	c.mlinkMu.Unlock()
	close(ready)

	wg, ctx := errgroup.WithContext(ctx)

	defer xio.CloseWhenDone(ctx, mlink)()

	// when we are done - reset .mlink
	defer func() {
		c.mlinkMu.Lock()
		c.mlink = nil
		c.mlinkMu.Unlock()
	}()

	// launch master notifications receiver
	wg.Go(func() error {
		return c.recvMaster(ctx, mlink)
	})

	// init partition table from master
	// XXX is this needed at all or we can expect master sending us pt via notify channel?
	wg.Go(func() error {
		return c.initFromMaster(ctx, mlink)
	})

	return wg.Wait()
}

// recvMaster receives and handles notifications from master
func (c *Client) recvMaster(ctx context.Context, mlink *neo.NodeLink) error {
	// XXX .nodeTab.Reset()

	for {
		req, err := mlink.Recv1()
		if err != nil {
			return err
		}
		err = req.Close()
		if err != nil {
			return err
		}

		c.node.StateMu.Lock()

		switch msg := req.Msg.(type) {
		default:
			c.node.StateMu.Unlock()
			return fmt.Errorf("unexpected message: %T", msg)

		// M sends whole PT
		case *neo.NotifyPartitionTable:
			pt := neo.PartTabFromDump(msg.PTid, msg.RowList)
			c.node.PartTab = pt

		// M sends δPT
		//case *neo.NotifyPartitionChanges:
			// TODO

		case *neo.NotifyNodeInformation:
			c.node.UpdateNodeTab(ctx, msg)

		case *neo.NotifyClusterState:
			c.node.UpdateClusterState(ctx, msg)
		}

		// update .operational + notify those who was waiting for it
		// XXX py client does not wait for cluster state = running
		operational := // c.node.ClusterState == neo.ClusterRunning &&
			c.node.PartTab.OperationalWith(c.node.NodeTab)

		var opready chan struct{}
		if operational != c.operational {
			c.operational = operational
			if operational {
				opready = c.opReady // don't close from under StateMu
			} else {
				c.opReady = make(chan struct{})
			}
		}

		c.node.StateMu.Unlock()

		if opready != nil {
			close(opready)
		}
	}
}

func (c *Client) initFromMaster(ctx context.Context, mlink *neo.NodeLink) error {
	// ask M for PT
	rpt := neo.AnswerPartitionTable{}
	err := mlink.Ask1(&neo.AskPartitionTable{}, &rpt)
	if err != nil {
		return err
	}

	pt := neo.PartTabFromDump(rpt.PTid, rpt.RowList)
	c.node.StateMu.Lock()
	c.node.PartTab = pt
	c.node.StateMu.Unlock()

/*
	XXX don't need this in init?

	// ask M about last_tid
	rlastTxn := neo.AnswerLastTransaction{}
	err = mlink.Ask1(&neo.LastTransaction{}, &rlastTxn)
	if err != nil {
		return err
	}

	// XXX lock
	// XXX rlastTxn.Tid -> c.lastTid
*/

	// XXX what next?
	return nil

	// TODO transaction control? -> better in original goroutines doing the txn (just share mlink)
}

// --- user API calls ---

func (c *Client) LastTid(ctx context.Context) (_ zodb.Tid, err error) {
	defer xerr.Context(&err, "client: lastTid")

	// XXX or require full withOperational ?
	mlink, err := c.masterLink(ctx)
	if err != nil {
		return 0, err
	}

	// XXX mlink can become down while we are making the call.
	// XXX do we want to return error or retry?
	reply := neo.AnswerLastTransaction{}
	err = mlink.Ask1(&neo.LastTransaction{}, &reply) // XXX Ask += ctx
	if err != nil {
		return 0, err	// XXX err ctx
	}
	return reply.Tid, nil
}

func (c *Client) LastOid(ctx context.Context) (zodb.Oid, error) {
	// XXX there is no LastOid in NEO/py
	panic("TODO")
}

func (c *Client) Load(ctx context.Context, xid zodb.Xid) (data []byte, serial zodb.Tid, err error) {
	defer xerr.Contextf(&err, "client: load %v", xid)	// XXX keep zodb errors intact?

	err = c.withOperational(ctx)
	if err != nil {
		return nil, 0, err
	}

	// here we have cluster state operational and rlocked. Retrieve
	// storages we might need to access and release the lock.
	storv := make([]*neo.Node, 0)
	for _, cell := range c.node.PartTab.Get(xid.Oid) {
		if cell.Readable() {
			stor := c.node.NodeTab.Get(cell.UUID)
			// this storage might not yet come up
			if stor != nil && stor.State == neo.RUNNING {
				storv = append(storv, stor)
			}
		}
	}
	c.node.StateMu.RUnlock()

	if len(storv) == 0 {
		// XXX recheck it adds traceback to log
		return nil, 0, errors.Errorf("internal inconsistency: cluster is operational, but no storages alive for oid %v", xid.Oid)
	}

	// XXX vvv temp stub -> TODO pick up 3 random storages and send load
	// requests to them all getting the first who is the fastest to reply;
	// retry from the beginning if all are found to fail?
	stor := storv[rand.Intn(len(storv))]

	slink, err := stor.Dial(ctx)
	if err != nil {
		return nil, 0, err	// XXX err ctx
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
	err = slink.Ask1(&req, &resp)
	if err != nil {
		return nil, 0, err	// XXX err context
	}

	checksum := sha1.Sum(data)
	if checksum != resp.Checksum {
		return nil, 0, fmt.Errorf("data corrupt: checksum mismatch")
	}

	data = resp.Data
	if resp.Compression {
		data, err = decompress(resp.Data, make([]byte, 0, len(resp.Data)))
		if err != nil {
			return nil, 0, fmt.Errorf("data corrupt: %v", err)
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
	// neo://name@master1,master2,...,masterN?options

	if u.User == nil {
		return nil, fmt.Errorf("neo: open %q: cluster name not specified")
	}

	// XXX check/use other url fields
	net := xnet.NetPlain("tcp")	// TODO + TLS; not only "tcp" ?

	// XXX we are not passing ctx to NewClient - right?
	//     as ctx for open can be done after open finishes - not covering
	//     whole storage working lifetime.
	return NewClient(u.User.Username(), u.Host, net), nil
}

func init() {
	zodb.RegisterStorage("neo", openClientByURL)
}
