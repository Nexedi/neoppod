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

// NEO. Packets and packet buffers management

package neo

import (
	"fmt"
	"unsafe"
)

// TODO organize rx buffers management (freelist etc)

// Buffer with packet data
type PktBuf struct {
	Data	[]byte	// whole packet data including all headers	XXX -> Buf ?
}

// XXX naming -> PktHeader ?
type PktHead struct {
	ConnId  be32	// NOTE is .msgid in py
	MsgCode be16
	Len	be32	// whole packet length (including header)
}

// Get pointer to packet header
func (pkt *PktBuf) Header() *PktHead {
	// XXX check len(Data) < PktHead ? -> no, Data has to be allocated with cap >= PktHeadLen
	return (*PktHead)(unsafe.Pointer(&pkt.Data[0]))
}

// Get packet payload
func (pkt *PktBuf) Payload() []byte {
	return pkt.Data[PktHeadLen:]
}


// packet dumping
func (pkt *PktBuf) String() string {
	if len(pkt.Data) < PktHeadLen {
		return fmt.Sprintf("(! < PktHeadLen) % x", pkt.Data)
	}

	h := pkt.Header()
	s := fmt.Sprintf(".%d", ntoh32(h.ConnId))

	msgCode := ntoh16(h.MsgCode)
	msgType := pktTypeRegistry[msgCode]
	if msgType == nil {
		s += fmt.Sprintf(" ? (%d)", msgCode)
	} else {
		s += fmt.Sprintf(" %s", msgType)
	}

	s += fmt.Sprintf(" #%d | ", ntoh32(h.Len))

	s += fmt.Sprintf("% x\n", pkt.Payload())	// XXX better decode
	return s
}
