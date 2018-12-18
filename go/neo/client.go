// Copyright (C) 2017-2018  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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

package neo
// client node with ZODB storage interface for accessing NEO cluster.

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/go123/xnet"

	"lab.nexedi.com/kirr/neo/go/internal/xzlib"
	"lab.nexedi.com/kirr/neo/go/neo/internal/xsha1"
	"lab.nexedi.com/kirr/neo/go/neo/neonet"
	"lab.nexedi.com/kirr/neo/go/neo/proto"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/internal/log"
	"lab.nexedi.com/kirr/neo/go/internal/task"
	"lab.nexedi.com/kirr/neo/go/internal/xio"
)

// Client is NEO node that talks to NEO cluster and exposes access to it via ZODB interfaces.
type Client struct {
	node *NodeApp

	talkMasterCancel func()

	// link to master - established and maintained by talkMaster.
	// users retrieve it via masterLink().
	mlinkMu    sync.Mutex
	mlink      *neonet.NodeLink
	mlinkReady chan struct{} // reinitialized at each new talk cycle

	// operational state in node is maintained by recvMaster.
	// users retrieve it via withOperational().
	//
	// NOTE being operational means:
	// - link to master established and is ok
	// - .PartTab is operational wrt .NodeTab
	// - .ClusterState = RUNNING	<- XXX needed?
	//
	// however master link is accessed separately (see ^^^ and masterLink)
	//
	// protected by .node.StateMu
	operational bool // XXX <- somehow move to NodeApp?
	opReady	    chan struct{} // reinitialized each time state becomes non-operational
}

var _ zodb.IStorageDriver = (*Client)(nil)

// NewClient creates new client node.
//
// It will connect to master @masterAddr and identify with specified cluster name.
func NewClient(clusterName, masterAddr string, net xnet.Networker) *Client {
	cli := newClient(clusterName, masterAddr, net)
	go cli.run(context.Background())	// XXX bg hardcoded
	return cli
}

func newClient(clusterName, masterAddr string, net xnet.Networker) *Client {
	return &Client{
		node:        NewNodeApp(net, proto.CLIENT, clusterName, masterAddr),
		mlinkReady:  make(chan struct{}),
		operational: false,
		opReady:     make(chan struct{}),
	}
}

// XXX make run public?
func (cli *Client) run(ctx context.Context) error {
	// run process which performs master talk
	ctx, cancel := context.WithCancel(ctx)
	cli.talkMasterCancel = cancel
	cli.node.OnShutdown = cancel // XXX ok?
	return cli.talkMaster(ctx)
}

func (c *Client) Close() error {
	c.talkMasterCancel()
	// XXX wait talkMaster finishes -> XXX return err from that?
	// XXX what else?
	return nil
}

// --- connection with master ---

// masterLink returns link to primary master.
//
// NOTE that even if masterLink returns != nil, the master link can become
// non-operational at any later time. (such cases will be reported as
// ErrLinkDown returned by all mlink operations)
func (c *Client) masterLink(ctx context.Context) (*neonet.NodeLink, error) {
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

// updateOperational updates .operational from current state.
//
// Must be called with .node.StateMu lock held.
//
// Returned sendReady func must be called by updateOperational caller after
// .node.StateMu lock is released - it will close current .opReady this way
// notifying .operational waiters.
//
// XXX move somehow -> NodeApp?
func (c *Client) updateOperational() (sendReady func()) {
	// XXX py client does not wait for cluster state = running
	operational := // c.node.ClusterState == ClusterRunning &&
		c.node.PartTab.OperationalWith(c.node.NodeTab)

	//fmt.Printf("\nupdateOperatinal: %v\n", operational)
	//fmt.Println(c.node.PartTab)
	//fmt.Println(c.node.NodeTab)

	var opready chan struct{}
	if operational != c.operational {
		c.operational = operational
		if operational {
			opready = c.opReady // don't close from under StateMu
		} else {
			c.opReady = make(chan struct{}) // remake for next operational waiters
		}
	}

	return func() {
		if opready != nil {
			//fmt.Println("updateOperational - notifying %v\n", opready)
			close(opready)
		}
	}
}

// withOperational waits for cluster state to be operational.
//
// If successful it returns with operational state RLocked (c.node.StateMu) and
// unlocked otherwise.
//
// The only error possible is if provided ctx cancel.
// XXX and client stopped/closed? (ctx passed to Run cancelled)
func (c *Client) withOperational(ctx context.Context) error {
	for {
		c.node.StateMu.RLock()
		if c.operational {
			return nil
		}

		ready := c.opReady
		c.node.StateMu.RUnlock()

		//fmt.Printf("withOperational - waiting on %v\n", ready)

		select {
		case <-ctx.Done():
			return ctx.Err()

		// XXX case <-c.runctx.Done():
		//	return "op on closed client ..." ?

		case <-ready:
			// ok - try to relock and read again.
		}
	}
}

// talkMaster connects to master, announces self and receives notifications.
// it tries to persist master link reconnecting as needed.
//
// XXX C -> M for commit	(-> another channel)
//
// XXX always error  (dup Storage.talkMaster) ?
func (c *Client) talkMaster(ctx context.Context) (err error) {
	defer task.Runningf(&ctx, "client: talk master(%v)", c.node.MasterAddr)(&err)

	// XXX dup wrt Storage.talkMaster
	for {
		err := c.talkMaster1(ctx)
		log.Warning(ctx, err)	// XXX Warning ok? -> Error?

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
	mlink, accept, err := c.node.Dial(ctx, proto.MASTER, c.node.MasterAddr)
	if err != nil {
		// FIXME it is not only identification - e.g. ECONNREFUSED
		return err
	}

	// FIXME vvv dup from Storage.talkMaster1

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
func (c *Client) recvMaster(ctx context.Context, mlink *neonet.NodeLink) (err error) {
	defer task.Running(&ctx, "rx")(&err)

	// XXX .nodeTab.Reset()

	for {
		req, err := mlink.Recv1()
		if err != nil {
			return err
		}
		err = c.recvMaster1(ctx, req)
		req.Close()
		if err != nil {
			return err
		}

	}
}

// recvMaster1 handles 1 message from master
func (c *Client) recvMaster1(ctx context.Context, req neonet.Request) error {
	c.node.StateMu.Lock()

	switch msg := req.Msg.(type) {
	default:
		c.node.StateMu.Unlock()
		return fmt.Errorf("unexpected message: %T", msg)

	// M sends whole PT
	case *proto.SendPartitionTable:
		c.node.UpdatePartTab(ctx, msg)

	// M sends δPT
	//case *proto.NotifyPartitionChanges:
		// TODO

	case *proto.NotifyNodeInformation:
		c.node.UpdateNodeTab(ctx, msg)

	case *proto.NotifyClusterState:
		c.node.UpdateClusterState(ctx, msg)
	}

	// update .operational + notify those who was waiting for it
	opready := c.updateOperational()
	c.node.StateMu.Unlock()
	opready()

	return nil
}

func (c *Client) initFromMaster(ctx context.Context, mlink *neonet.NodeLink) (err error) {
	defer task.Running(&ctx, "init")(&err)

	// ask M for PT
	rpt := proto.AnswerPartitionTable{}
	err = mlink.Ask1(&proto.AskPartitionTable{}, &rpt)
	if err != nil {
		return err
	}

	pt := PartTabFromDump(rpt.PTid, rpt.RowList)
	log.Infof(ctx, "master initialized us with next parttab:\n%s", pt)
	c.node.StateMu.Lock()
	c.node.PartTab = pt
	opready := c.updateOperational()
	c.node.StateMu.Unlock()
	opready()

/*
	XXX don't need this in init?

	// ask M about last_tid
	rlastTxn := AnswerLastTransaction{}
	err = mlink.Ask1(&LastTransaction{}, &rlastTxn)
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
	defer func() {
		if err != nil {
			err = &zodb.OpError{URL: c.URL(), Op: "last_tid", Args: nil, Err: err}
		}
	}()

	// XXX or require full withOperational ?
	mlink, err := c.masterLink(ctx)
	if err != nil {
		return 0, err
	}

	// XXX mlink can become down while we are making the call.
	// XXX do we want to return error or retry?
	reply := proto.AnswerLastTransaction{}
	err = mlink.Ask1(&proto.LastTransaction{}, &reply) // XXX Ask += ctx
	if err != nil {
		// XXX ZODBErrDecode?
		return 0, err	// XXX err ctx
	}
	return reply.Tid, nil
}

func (c *Client) Load(ctx context.Context, xid zodb.Xid) (buf *mem.Buf, serial zodb.Tid, err error) {
	// defer func() ...
	buf, serial, err = c._Load(ctx, xid)
	if err != nil {
		err = &zodb.OpError{URL: c.URL(), Op: "load", Args: xid, Err: err}
	}
	return buf, serial, err
}

func (c *Client) _Load(ctx context.Context, xid zodb.Xid) (*mem.Buf, zodb.Tid, error) {
	err := c.withOperational(ctx)
	if err != nil {
		return nil, 0, err
	}

	// here we have cluster state operational and rlocked. Retrieve
	// storages we might need to access and release the lock.
	storv := make([]*Node, 0, 1)
	for _, cell := range c.node.PartTab.Get(xid.Oid) {
		if cell.Readable() {
			stor := c.node.NodeTab.Get(cell.UUID)
			// this storage might not yet come up
			if stor != nil && stor.State == proto.RUNNING {
				storv = append(storv, stor)
			}
		}
	}
	c.node.StateMu.RUnlock()

	if len(storv) == 0 {
		// XXX recheck it adds traceback to log -> XXX it does not -> add our Bugf which always forces +v on such error print
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
	// FIXME ^^^ slink.CloseAccept after really dialed (not to deadlock if
	// S decides to send us something)

	// on the wire it comes as "before", not "at"
	req := proto.GetObject{
		Oid:    xid.Oid,
		Tid:    at2Before(xid.At),
		Serial: proto.INVALID_TID,
	}

	resp := proto.AnswerObject{}
	err = slink.Ask1(&req, &resp)
	if err != nil {
		if e, ok := err.(*proto.Error); ok {
			err = proto.ZODBErrDecode(e)
		}
		return nil, 0, err	// XXX err context
	}

	buf := resp.Data

	if !xsha1.Skip {
		checksum := xsha1.Sum(buf.Data)
		if checksum != resp.Checksum {
			return nil, 0, fmt.Errorf("data corrupt: checksum mismatch")
		}
	}

	if resp.Compression {
		buf2 := &mem.Buf{Data: nil}
		udata, err := xzlib.Decompress(buf.Data)
		buf.Release()
		if err != nil {
			return nil, 0, fmt.Errorf("data corrupt: %v", err)
		}
		buf2.Data = udata
		buf = buf2
	}

	// reply.NextSerial
	// reply.DataSerial
	return buf, resp.Serial, nil
}

func (c *Client) Iterate(ctx context.Context, tidMin, tidMax zodb.Tid) zodb.ITxnIterator {
	// see notes in ../NOTES:"On iteration"
	panic("TODO")
}

func (c *Client) Watch(ctx context.Context) (zodb.Tid, []zodb.Oid, error) {
	panic("TODO")
}


// ---- ZODB open/url support ----


func openClientByURL(ctx context.Context, u *url.URL, opt *zodb.DriverOptions) (zodb.IStorageDriver, error) {
	// neo://name@master1,master2,...,masterN?options

	if u.User == nil {
		return nil, fmt.Errorf("neo: open %q: cluster name not specified", u)
	}

	// XXX readonly stub
	// XXX place = ?
	if !opt.ReadOnly {
		return nil, fmt.Errorf("neo: %s: TODO write mode not implemented", u)
	}

	// XXX handle opt.WatchQ

	// XXX check/use other url fields
	net := xnet.NetPlain("tcp")	// TODO + TLS; not only "tcp" ?

	// XXX we are not passing ctx to NewClient - right?
	//     as ctx for open can be done after open finishes - not covering
	//     whole storage working lifetime.
	c := NewClient(u.User.Username(), u.Host, net)
	return c, nil
}

func (c *Client) URL() string {
	// XXX neos:// depending whether it was tls
	// XXX options if such were given to open are discarded
	//     (but we need to be able to construct URL if Client was created via NewClient directly)
	return fmt.Sprintf("neo://%s@%s", c.node.ClusterName, c.node.MasterAddr)
}

func init() {
	zodb.RegisterDriver("neo", openClientByURL)
}
