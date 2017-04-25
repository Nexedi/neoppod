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
// NEO storage node

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"../zodb"
	"../zodb/storage/fs1"
)

// NEO Storage application

// XXX naming
type Storage struct {
	zstor zodb.IStorage // underlying ZODB storage	XXX temp ?
}

func NewStorage(zstor zodb.IStorage) *Storage {
	return &Storage{zstor}
}


/*
// XXX change to bytes.Buffer if we need to access it as I/O
type Buffer struct {
	buf	[]byte
}
*/

func (stor *Storage) ServeLink(ctx context.Context, link *NodeLink) {
	fmt.Printf("stor: serving new node %s <-> %s\n", link.peerLink.LocalAddr(), link.peerLink.RemoteAddr())

/*
	pktri, err := expect(RequestIdentification)
	if err != nil {
		send(err)
		return
	}

	if pktri.ProtocolVersion != PROTOCOL_VERSION {
		sendErr("...")
		return
	}

	(.NodeType, .UUID, .Address, .Name, .IdTimestamp) -> check + register to NM

	send(AcceptIdentification{...})
	// TODO mark link as identified


	pkt, err := recv()
	if err != nil {
		err
		return
	}

	switch pkt.MsgCode {
	case GetObject:
		req := GetObject{}
		err = req.NEODecode(pkt.Payload())
		if err != nil {
			sendErr("malformed GetObject packet:", err)
		}

		-> DM.getObject(req.Oid, req.Serial, req.Tid)

	case StoreObject:
	case StoreTransaction:
	}
*/



	//fmt.Fprintf(conn, "Hello up there, you address is %s\n", conn.RemoteAddr())	// XXX err
	//conn.Close()	// XXX err

	/*
	// TODO: use bytes.Buffer{}
	//	 .Bytes() -> buf -> can grow up again up to buf[:cap(buf)]
	//	 NewBuffer(buf) -> can use same buffer for later reading via bytes.Buffer
	// TODO read PktHeader (fixed length)  (-> length, PktType (by .code))
	//rxbuf := bytes.Buffer{}
	rxbuf := bytes.NewBuffer(make([]byte, 4096))
	n, err := conn.Read(rxbuf.Bytes())
	*/

	//recvPkt()
}


// XXX naming for RecvAndDecode and EncodeAndSend

// XXX stub
// XXX move me out of here
func RecvAndDecode(conn *Conn) (interface{}, error) {	// XXX interface{} -> NEODecoder ?
	pkt, err := conn.Recv()
	if err != nil {
		return nil, err
	}

	// TODO decode pkt
	return pkt, nil
}

func EncodeAndSend(conn *Conn, pkt NEOEncoder) error {
	// TODO encode pkt
	msgCode, l := pkt.NEOEncodedInfo()
	l += PktHeadLen
	buf := PktBuf{make([]byte, l)}	// XXX -> freelist
	h := buf.Header()
	h.MsgCode = hton16(msgCode)
	h.Len = hton32(uint32(l))	// XXX casting: think again

	pkt.NEOEncode(buf.Payload())

	return conn.Send(&buf)	// XXX why pointer?
}

// ServeClient serves incoming connection on which peer identified itself as client
func (stor *Storage) ServeClient(ctx context.Context, conn *Conn) {
	// close connection when either cancelling or returning (e.g. due to an error)
	// ( when cancelling - conn.Close will signal to current IO to
	//   terminate with an error )
	retch := make(chan struct{})
	defer func() { close(retch) }()
	go func() {
		select {
		case <-ctx.Done():
			// XXX tell client we are shutting down?
		case <-retch:
		}
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
				reply = &Error{Code: 0, Message: err.Error()}	// XXX Code
			} else {
				reply = &AnswerGetObject{
						Oid:	 xid.Oid,
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
				reply = &Error{Code:0, Message: err.Error()}
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

const storageSummary = "run NEO storage node"

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

	//flags := flag.FlagSet{Usage: func() { storageUsage(os.Stderr) }}
	//flags.Init("", flag.ExitOnError)
	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.Usage = func() { storageUsage(os.Stderr); flags.PrintDefaults() }	// XXX prettify
	flags.StringVar(&bind, "bind", bind, "address to serve on")
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		os.Exit(2)
	}

	// XXX hack
	zstor, err := fs1.Open(argv[0])
	if err != nil {
		log.Fatal(err)
	}

	storsrv := NewStorage(zstor)

	/*
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Second)
		cancel()
	}()
	*/
	ctx := context.Background()

	err = ListenAndServe(ctx, "tcp", bind, storsrv)	// XXX hardcoded
	if err != nil {
		log.Fatal(err)
	}
}
