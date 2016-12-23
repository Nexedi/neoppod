// Copyright (C) 2016  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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

package neo

import (
	"context"
	"fmt"
)

// NEO Storage application

// XXX naming
type StorageApplication struct {
}


/*
// XXX change to bytes.Buffer if we need to access it as I/O
type Buffer struct {
	buf	[]byte
}
*/

func (stor *StorageApplication) ServeLink(ctx context.Context, link *NodeLink) {
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
		err = req.Decode(pkt.Payload())
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
