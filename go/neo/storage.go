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

	//"time"

	"../zodb"
	"../zodb/storage/fs1"
)

// XXX fmt -> log

// Storage is NEO storage server application
type Storage struct {
	zstor zodb.IStorage // underlying ZODB storage	XXX temp ?
}

func NewStorage(zstor zodb.IStorage) *Storage {
	return &Storage{zstor}
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
	var bind string

	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.Usage = func() { storageUsage(os.Stderr); flags.PrintDefaults() }	// XXX prettify
	flags.StringVar(&bind, "bind", bind, "address to serve on")
	flags.Parse(argv[1:])

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

	storSrv := NewStorage(zstor)

	ctx := context.Background()
	/*
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()
	*/

	// TODO + TLS
	err = ListenAndServe(ctx, "tcp", bind, storSrv)	// XXX "tcp" hardcoded
	if err != nil {
		log.Fatal(err)
	}
}
