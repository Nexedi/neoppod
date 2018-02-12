// Copyright (C) 2016-2018  Nexedi SA and Contributors.
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

package neonet
// packets and packet buffers management

import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	"lab.nexedi.com/kirr/go123/xbytes"

	"lab.nexedi.com/kirr/neo/go/neo/proto"
	"lab.nexedi.com/kirr/neo/go/xcommon/packed"
)

// pktBuf is a buffer with full raw packet (header + data).
//
// Allocate pktBuf via pktAlloc() and free via pktBuf.Free().
type pktBuf struct {
	Data []byte // whole packet data including all headers
}

// Header returns pointer to packet header.
func (pkt *pktBuf) Header() *proto.PktHeader {
	// XXX check len(Data) < PktHeader ? -> no, Data has to be allocated with cap >= PktHeaderLen
	return (*proto.PktHeader)(unsafe.Pointer(&pkt.Data[0]))
}

// Payload returns []byte representing packet payload.
func (pkt *pktBuf) Payload() []byte {
	return pkt.Data[proto.PktHeaderLen:]
}

// ---- pktBuf freelist ----

// pktBufPool is sync.Pool<pktBuf>
var pktBufPool = sync.Pool{New: func() interface{} {
	return &pktBuf{Data: make([]byte, 0, 4096)}
}}

// pktAlloc allocates pktBuf with len=n
func pktAlloc(n int) *pktBuf {
	pkt := pktBufPool.Get().(*pktBuf)
	pkt.Data = xbytes.Realloc(pkt.Data, n)
	return pkt
}

// Free marks pkt as no longer needed.
func (pkt *pktBuf) Free() {
	pktBufPool.Put(pkt)
}


// ---- pktBuf dump ----

// Strings dumps a packet in human-readable form
func (pkt *pktBuf) String() string {
	if len(pkt.Data) < proto.PktHeaderLen {
		return fmt.Sprintf("(! < PktHeaderLen) % x", pkt.Data)
	}

	h := pkt.Header()
	s := fmt.Sprintf(".%d", packed.Ntoh32(h.ConnId))

	msgCode := packed.Ntoh16(h.MsgCode)
	msgLen  := packed.Ntoh32(h.MsgLen)
	data    := pkt.Payload()
	msgType := proto.MsgType(msgCode)
	if msgType == nil {
		s += fmt.Sprintf(" ? (%d) #%d [%d]: % x", msgCode, msgLen, len(data), data)
		return s
	}

	// XXX dup wrt Conn.Recv
	msg := reflect.New(msgType).Interface().(proto.Msg)
	n, err := msg.NEOMsgDecode(data)
	if err != nil {
		s += fmt.Sprintf(" (%s) %v; #%d [%d]: % x", msgType, err, msgLen, len(data), data)
	}

	s += fmt.Sprintf(" %s %v", msgType.Name(), msg)	// XXX or %+v better?

	if n < len(data) {
		tail := data[n:]
		s += fmt.Sprintf(" ;  [%d]tail: % x", len(tail), tail)
	}

	return s
}

// Dump dumps a packet in raw form
func (pkt *pktBuf) Dump() string {
	if len(pkt.Data) < proto.PktHeaderLen {
		return fmt.Sprintf("(! < pktHeaderLen) % x", pkt.Data)
	}

	h := pkt.Header()
	data := pkt.Payload()
	return fmt.Sprintf(".%d (%d) #%d [%d]: % x",
		packed.Ntoh32(h.ConnId), packed.Ntoh16(h.MsgCode), packed.Ntoh32(h.MsgLen), len(data), data)
}
