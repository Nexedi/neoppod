// Copyright (C) 2016-2017  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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
// storage node

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"../zodb"
	"../zodb/storage/fs1"
)

// XXX fmt -> log

// Storage is NEO storage server application
type Storage struct {
	// XXX move -> nodeCommon?
	// ---- 8< ----
	myInfo		NodeInfo	// XXX -> only Address + NodeUUID ?
	clusterName	string

	net		Network		// network we are sending/receiving on
	masterAddr	string		// address of master	XXX -> Address ?
	// ---- 8< ----

	zstor zodb.IStorage // underlying ZODB storage	XXX temp ?
}

// NewStorage creates new storage node that will listen on serveAddr and talk to master on masterAddr
// The storage uses zstor as underlying backend for storing data.
// To actually start running the node - call Run.	XXX text
func NewStorage(cluster string, masterAddr string, serveAddr string, net Network, zstor zodb.IStorage) *Storage {
	// convert serveAddr into neo format
	addr, err := ParseAddress(serveAddr)
	if err != nil {
		panic(err)	// XXX
	}

	stor := &Storage{
			myInfo:		NodeInfo{NodeType: STORAGE, Address: addr},
			clusterName:	cluster,
			net:		net,
			masterAddr:	masterAddr,
			zstor:		zstor,
	}

	return stor
}


// Run starts storage node and runs it until either ctx is cancelled or master
// commands it to shutdown.
func (stor *Storage) Run(ctx context.Context) error {
	// start listening
	l, err := stor.net.Listen(stor.myInfo.Address.String())		// XXX ugly
	if err != nil {
		return err // XXX err ctx
	}

	// FIXME -> no -> Serve closes l
	defer l.Close()	// XXX err ?

	// now we know our listening address (in case it was autobind before)
	// NOTE listen("tcp", ":1234") gives l.Addr 0.0.0.0:1234 and
	//      listen("tcp6", ":1234") gives l.Addr [::]:1234
	//	-> host is never empty
	addr, err := ParseAddress(l.Addr().String())
	if err != nil {
		// XXX -> panic here ?
		return err	// XXX err ctx
	}

	stor.myInfo.Address = addr

	go stor.talkMaster(ctx)

	err = Serve(ctx, l, stor)	// XXX -> go ?
	return err // XXX err ctx

	// XXX oversee both master and server and wait ?
}

// talkMaster connects to master, announces self and receives notifications and commands
// XXX and notifies master about ? (e.g. StartOperation -> NotifyReady)
// it tries to persist master link reconnecting as needed
func (stor *Storage) talkMaster(ctx context.Context) {
	for {
		fmt.Printf("stor: master(%v): connecting\n", stor.masterAddr) // XXX info
		err := stor.talkMaster1(ctx)
		fmt.Printf("stor: master(%v): %v\n", stor.masterAddr, err)

		// XXX handle shutdown command from master

		// throttle reconnecting / exit on cancel
		select {
		case <-ctx.Done():
			return

		// XXX 1s hardcoded -> move out of here
		case <-time.After(1*time.Second):
			// ok
		}
	}
}

// talkMaster1 does 1 cycle of connect/talk/disconnect to master
// it returns error describing why such cycle had to finish
// XXX distinguish between temporary problems and non-temporary ones?
func (stor *Storage) talkMaster1(ctx context.Context) error {
	Mlink, err := Dial(ctx, stor.net, stor.masterAddr)
	if err != nil {
		return err
	}

	// TODO Mlink.Close() on return / cancel

	// request identification this way registering our node to master
	accept, err := IdentifyWith(MASTER, Mlink, stor.myInfo, stor.clusterName)
	if err != nil {
		return err
	}

	// XXX add master UUID -> nodeTab ? or master will notify us with it himself ?

	if !(accept.NumPartitions == 1 && accept.NumReplicas == 1) {
		return fmt.Errorf("TODO for 1-storage POC: Npt: %v  Nreplica: %v", accept.NumPartitions, accept.NumReplicas)
	}

	if accept.YourNodeUUID != stor.myInfo.NodeUUID {
		fmt.Printf("stor: %v: master told us to have UUID=%v\n", Mlink, accept.YourNodeUUID)
		stor.myInfo.NodeUUID = accept.YourNodeUUID
		// XXX notify anyone?
	}

	// now handle notifications and commands from master
	// FIXME wrong - either keep conn as one used from identification or accept from listening
	conn, err := Mlink.NewConn()
	if err != nil { panic(err) }	// XXX

	for {
		notify, err := RecvAndDecode(conn)
		if err != nil {
			// XXX TODO
		}

		_ = notify	// XXX temp
	}





}

// ServeLink serves incoming node-node link connection
// XXX +error return?
func (stor *Storage) ServeLink(ctx context.Context, link *NodeLink) {
	fmt.Printf("stor: %s: serving new node\n", link)

	// close link when either cancelling or returning (e.g. due to an error)
	// ( when cancelling - link.Close will signal to all current IO to
	//   terminate with an error )
	// XXX dup -> utility
	retch := make(chan struct{})
	defer func() { close(retch) }()
	go func() {
		select {
		case <-ctx.Done():
			// XXX tell peers we are shutting down?
			// XXX ret err = ctx.Err()
		case <-retch:
		}
		fmt.Printf("stor: %v: closing link\n", link)
		link.Close()	// XXX err
	}()

	// XXX recheck identification logic here
	nodeInfo, err := IdentifyPeer(link, STORAGE)
	if err != nil {
		fmt.Printf("stor: %v\n", err)
		return
	}

	var serveConn func(context.Context, *Conn)
	switch nodeInfo.NodeType {
	case CLIENT:
		serveConn = stor.ServeClient

	default:
		fmt.Printf("stor: %v: unexpected peer type: %v\n", link, nodeInfo.NodeType)
		return
	}

	// identification passed, now serve other requests
	for {
		conn, err := link.Accept()
		if err != nil {
			fmt.Printf("stor: %v\n", err)	// XXX err ctx
			break
		}

		// XXX adjust ctx ?
		// XXX wrap conn close to happen here, not in ServeClient ?
		go serveConn(ctx, conn)
	}

	// TODO wait all spawned serveConn
}


// XXX move err{Encode,Decode} out of here

// errEncode translates an error into Error packet
func errEncode(err error) *Error {
	switch err := err.(type) {
	case *Error:
		return err
	case *zodb.ErrXidMissing:
		// XXX abusing message for xid
		return &Error{Code: OID_NOT_FOUND, Message: err.Xid.String()}

	default:
		return &Error{Code: NOT_READY /* XXX how to report 503? was BROKEN_NODE */, Message: err.Error()}
	}

}

// errDecode decodes error from Error packet
func errDecode(e *Error) error {
	switch e.Code {
	case OID_NOT_FOUND:
		xid, err := zodb.ParseXid(e.Message)	// XXX abusing message for xid
		if err == nil {
			return &zodb.ErrXidMissing{xid}
		}
	}

	return e
}

func (stor *Storage) ServeMaster(ctx context.Context, conn *Conn) {

	// state changes:
	//
	// - Recovery
	// - StartOperation
	// - StopOperation
	// ? NotifyClusterInformation

	// - NotifyNodeInformation (e.g. M tells us we are RUNNING)
	// ? NotifyPartitionTable
}

// ServeClient serves incoming connection on which peer identified itself as client
// XXX +error return?
func (stor *Storage) ServeClient(ctx context.Context, conn *Conn) {
	fmt.Printf("stor: %s: serving new client conn\n", conn)

	// close connection when either cancelling or returning (e.g. due to an error)
	// ( when cancelling - conn.Close will signal to current IO to
	//   terminate with an error )
	// XXX dup -> utility
	retch := make(chan struct{})
	defer func() { close(retch) }()
	go func() {
		select {
		case <-ctx.Done():
			// XXX tell client we are shutting down?
			// XXX ret err = cancelled ?
		case <-retch:
		}
		fmt.Printf("stor: %v: closing client conn\n", conn)
		conn.Close()	// XXX err
	}()

	for {
		req, err := RecvAndDecode(conn)
		if err != nil {
			return	// XXX log / err / send error before closing
		}

		switch req := req.(type) {
		case *GetObject:
			xid := zodb.Xid{Oid: req.Oid}
			if req.Serial != INVALID_TID {
				xid.Tid = req.Serial
				xid.TidBefore = false
			} else {
				xid.Tid = req.Tid
				xid.TidBefore = true
			}

			var reply NEOEncoder
			data, tid, err := stor.zstor.Load(xid)
			if err != nil {
				// TODO translate err to NEO protocol error codes
				reply = errEncode(err)
			} else {
				reply = &AnswerGetObject{
						Oid:	xid.Oid,
						Serial: tid,

						Compression: false,
						Data: data,
						// XXX .CheckSum

						// XXX .NextSerial
						// XXX .DataSerial
					}
			}

			EncodeAndSend(conn, reply)	// XXX err

		case *LastTransaction:
			var reply NEOEncoder

			lastTid, err := stor.zstor.LastTid()
			if err != nil {
				reply = errEncode(err)
			} else {
				reply = &AnswerLastTransaction{lastTid}
			}

			EncodeAndSend(conn, reply)	// XXX err

		//case *ObjectHistory:
		//case *StoreObject:

		default:
			panic("unexpected packet")	// XXX
		}

		//req.Put(...)
	}
}

// ----------------------------------------

const storageSummary = "run storage node"

// TODO options:
// cluster, masterv ...

func storageUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: neo storage [options] zstor	XXX
Run NEO storage node.
`)

	// FIXME use w (see flags.SetOutput)
}

func storageMain(argv []string) {
	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.Usage = func() { storageUsage(os.Stderr); flags.PrintDefaults() }	// XXX prettify
	cluster := flags.String("cluster", "", "the cluster name")
	masters := flags.String("masters", "", "list of masters")
	bind := flags.String("bind", "", "address to serve on")
	flags.Parse(argv[1:])

	if *cluster == "" {
		// XXX vvv -> die  or  log.Fatalf ?
		fmt.Fprintf(os.Stderr, "cluster name must be provided")
		os.Exit(2)
	}

	masterv := strings.Split(*masters, ",")
	if len(masterv) == 0 {
		fmt.Fprintf(os.Stderr, "master list must be provided")
		os.Exit(2)
	}
	if len(masterv) > 1 {
		fmt.Fprintf(os.Stderr, "BUG neo/go POC currently supports only 1 master")
		os.Exit(2)
	}

	master := masterv[0]

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		os.Exit(2)
	}

	// XXX hack to use existing zodb storage for data
	zstor, err := fs1.Open(context.Background(), argv[0])	// XXX context.Background -> ?
	if err != nil {
		log.Fatal(err)
	}

	net := NetPlain("tcp")	// TODO + TLS; not only "tcp" ?

	storSrv := NewStorage(*cluster, master, *bind, net, zstor)

	ctx := context.Background()
	/*
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()
	*/

	err = storSrv.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
