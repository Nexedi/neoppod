// Copyright (C) 2016-2017  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 2, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

// TODO text
// XXX move to separate "storage" package ?

package neo

import (
	"context"
	"fmt"

	"../zodb"
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
	l := pkt.NEOEncodedLen()
	buf := PktBuf{make([]byte, PktHeadLen + l)}	// XXX -> freelist
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
				reply = &Error{Code: 0, Message: err.Error()}
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
			lastTid := stor.zstor.LastTid()
			EncodeAndSend(conn, &AnswerLastTransaction{lastTid})	// XXX err

		//case *ObjectHistory:
		//case *StoreObject:
		}

		//req.Put(...)
	}
}
