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
// packets and packet buffers management

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
	MsgCode be16	// payload message code
	MsgLen  be32	// payload message length (excluding packet header)
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

	s += fmt.Sprintf(" #%d | ", ntoh32(h.MsgLen))

	s += fmt.Sprintf("% x", pkt.Payload())	// XXX better decode
	return s
}
