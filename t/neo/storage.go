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
	"net"
	"fmt"

	//"../neo/proto"
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
