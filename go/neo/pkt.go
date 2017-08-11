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
	"unsafe"
)

// TODO organize rx buffers management (freelist etc)

// PktBuf is a buffer with full raw packet (header + data)
//
// variables of type PktBuf are usually named "pkb" (packet buffer), similar to "skb" in Linux
type PktBuf struct {
	Data	[]byte	// whole packet data including all headers	XXX -> Buf ?
}

// PktHead represents header of a raw packet
// XXX naming -> PktHeader ?
type PktHead struct {
	ConnId  be32	// NOTE is .msgid in py
	MsgCode be16	// payload message code
	MsgLen  be32	// payload message length (excluding packet header)
}

// Header returns pointer to packet header
func (pkt *PktBuf) Header() *PktHead {
	// XXX check len(Data) < PktHead ? -> no, Data has to be allocated with cap >= PktHeadLen
	return (*PktHead)(unsafe.Pointer(&pkt.Data[0]))
}

// Payload returns []byte representing packet payload
func (pkt *PktBuf) Payload() []byte {
	return pkt.Data[PktHeadLen:]
}


// Strings dumps a packet in human-readable form
func (pkt *PktBuf) String() string {
	if len(pkt.Data) < PktHeadLen {
		return fmt.Sprintf("(! < PktHeadLen) % x", pkt.Data)
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

	s += fmt.Sprintf(" %s %+v", msgType.Name(), msg)

	if n < len(data) {
		tail := data[n:]
		s += fmt.Sprintf(" ;  [%d]tail: % x", len(tail), tail)
	}

	return s
}

// Dump dumps a packet in raw form
func (pkt *PktBuf) Dump() string {
	if len(pkt.Data) < PktHeadLen {
		return fmt.Sprintf("(! < PktHeadLen) % x", pkt.Data)
	}

	h := pkt.Header()
	data := pkt.Payload()
	return fmt.Sprintf(".%d (%d) #%d [%d]: % x",
		ntoh32(h.ConnId), ntoh16(h.MsgCode), ntoh32(h.MsgLen), len(data), data)
}
