// Copyright (C) 2016-2017  Nexedi SA and Contributors.
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
// packets and packet buffers management

import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	"lab.nexedi.com/kirr/go123/xbytes"
)

// PktBuf is a buffer with full raw packet (header + data).
//
// variables of type PktBuf are usually named "pkb" (packet buffer), similar to "skb" in Linux.
//
// Allocate PktBuf via allocPkt() and free via PktBuf.Free().
type PktBuf struct {
	Data []byte // whole packet data including all headers
}

// Header returns pointer to packet header.
func (pkt *PktBuf) Header() *PktHeader {
	// XXX check len(Data) < PktHeader ? -> no, Data has to be allocated with cap >= pktHeaderLen
	return (*PktHeader)(unsafe.Pointer(&pkt.Data[0]))
}

// Payload returns []byte representing packet payload.
func (pkt *PktBuf) Payload() []byte {
	return pkt.Data[pktHeaderLen:]
}

// Resize resizes pkt to be of length n.
//
// semantic = xbytes.Resize.
func (pkt *PktBuf) Resize(n int) {
	pkt.Data = xbytes.Resize(pkt.Data, n)
}

// ---- PktBuf freelist ----

// pktBufPool is sync.Pool<pktBuf>
var pktBufPool = sync.Pool{New: func() interface{} {
	return &PktBuf{Data: make([]byte, 0, 4096)}
}}

// pktAlloc allocates PktBuf with len=n
func pktAlloc(n int) *PktBuf {
	pkt := pktBufPool.Get().(*PktBuf)
	pkt.Data = xbytes.Realloc(pkt.Data, n)
	return pkt
}

// Free marks pkt as no longer needed.
func (pkt *PktBuf) Free() {
	pktBufPool.Put(pkt)
}


// ---- PktBuf dump ----

// Strings dumps a packet in human-readable form
func (pkt *PktBuf) String() string {
	if len(pkt.Data) < pktHeaderLen {
		return fmt.Sprintf("(! < pktHeaderLen) % x", pkt.Data)
	}

	h := pkt.Header()
	s := fmt.Sprintf(".%d", ntoh32(h.ConnId))

	msgCode := ntoh16(h.MsgCode)
	msgLen  := ntoh32(h.MsgLen)
	data    := pkt.Payload()
	msgType := msgTypeRegistry[msgCode]
	if msgType == nil {
		s += fmt.Sprintf(" ? (%d) #%d [%d]: % x", msgCode, msgLen, len(data), data)
		return s
	}

	// XXX dup wrt Conn.Recv
	msg := reflect.New(msgType).Interface().(Msg)
	n, err := msg.neoMsgDecode(data)
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
func (pkt *PktBuf) Dump() string {
	if len(pkt.Data) < pktHeaderLen {
		return fmt.Sprintf("(! < pktHeaderLen) % x", pkt.Data)
	}

	h := pkt.Header()
	data := pkt.Payload()
	return fmt.Sprintf(".%d (%d) #%d [%d]: % x",
		ntoh32(h.ConnId), ntoh16(h.MsgCode), ntoh32(h.MsgLen), len(data), data)
}
