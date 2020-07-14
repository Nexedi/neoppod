// Copyright (C) 2018-2020  Nexedi SA and Contributors.
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

package zeo
// Protocol for exchanged ZEO messages.
// On the wire messages are encoded via pickles.
// Each message is wrapped into packet with be32 header of whole packet size.
// See https://github.com/zopefoundation/ZEO/blob/5.2.1-20-gcb26281d/doc/protocol.rst for details.

import (
	"bytes"
	"encoding/binary"
	"fmt"

	pickle  "github.com/kisielk/og-rek"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/internal/pickletools"
)

// msg represents 1 message.
// arg is arbitrary argument(s) passed/received along ZEO call or reply.
//
// for objects in arg user code has to obtain them via encoding.as*() and
// set them via encoding.Tid(), encoding.Oid() and other similar methods that
// convert application-level data into objects properly corresponding to wire
// encoding of messages.
type msg struct {
	msgid  int64
	flags  msgFlags
	method string
	arg    interface{} // can be e.g. (arg1, arg2, ...)
}

type msgFlags int64
const (
	msgAsync  msgFlags = 1 // message does not need a reply
	msgExcept          = 2 // exception was raised on remote side (ZEO5)
)

// encoding represents messages encoding.
type encoding byte // Z - pickles

// ---- message encode/decode ↔ packet ----

// pktEncode encodes message into raw packet.
func (e encoding) pktEncode(m msg) *pktBuf {
	switch e {
	case 'Z': return pktEncodeZ(m)
	default:  panic("bug")
	}
}

// pktDecode decodes raw packet into message.
func (e encoding) pktDecode(pkb *pktBuf) (msg, error) {
	switch e {
	case 'Z': return pktDecodeZ(pkb)
	default:  panic("bug")
	}
}


// pktEncodeZ encodes message into raw Z (pickle) packet.
func pktEncodeZ(m msg) *pktBuf {
	pkb := allocPkb()
	p := pickle.NewEncoder(pkb)
	err := p.Encode(pickle.Tuple{m.msgid, m.flags, m.method, m.arg})
	if err != nil {
		panic(err) // all our types are expected to be supported by pickle
	}
	return pkb
}

// pktDecodeZ decodes raw Z (pickle) packet into message.
func pktDecodeZ(pkb *pktBuf) (msg, error) {
	var m msg
	// must be (msgid, False|0, ".reply", res)
	d := pickle.NewDecoder(bytes.NewReader(pkb.Payload()))
	xpkt, err := d.Decode()
	if err != nil {
		return m, err
	}

	tpkt, ok := encoding('Z').asTuple(xpkt)
	if !ok {
		return m, derrf("got %T; expected tuple", xpkt)
	}
	if len(tpkt) != 4 {
		return m, derrf("len(msg-tuple)=%d; expected 4", len(tpkt))
	}
	m.msgid, ok = pickletools.Xint64(tpkt[0])
	if !ok {
		return m, derrf("msgid: got %T; expected int", tpkt[0])
	}

	flags, ok := pickletools.Xint64(tpkt[1])
	if !ok {
		bflags, ok := tpkt[1].(bool)
		if !ok {
			return m, derrf("flags: got %T; expected int|bool", tpkt[1])
		}

		if bflags {
			flags = 1
		} // else: flags is already = 0
	}
	// XXX check flags are in range?
	m.flags = msgFlags(flags)

	m.method, ok = tpkt[2].(string)
	if !ok {
		return m, derrf(".%d: method: got %T; expected str", m.msgid, tpkt[2])
	}

	m.arg = tpkt[3]
	return m, nil
}


func derrf(format string, argv ...interface{}) error {
	return fmt.Errorf("decode: "+format, argv...)
}


// ---- retrieve/put objects from/into msg.arg ----

// tuple represents py tuple.
type tuple []interface{}

// Tuple converts t into corresponding object appropriate for encoding e.
func (e encoding) Tuple(t tuple) pickle.Tuple {
	switch e {
	default:
		panic("bug")

	case 'Z':
		// pickle: -> pickle.Tuple
		return pickle.Tuple(t)
	}
}

// asTuple tries to retrieve tuple from corresponding object decoded via encoding e.
func (e encoding) asTuple(xt interface{}) (tuple, bool) {
	switch e {
	default:
		panic("bug")

	case 'Z':
		// pickle: tuples are represented by pickle.Tuple; lists as []interface{}
		switch t := xt.(type) {
		case pickle.Tuple:
			return tuple(t), true
		case []interface{}:
			return tuple(t), true
		default:
			return tuple(nil), false
		}
	}
}


// xuint64Unpack tries to decode packed 8-byte string as bigendian uint64
func (e encoding) xuint64Unpack(xv interface{}) (uint64, bool) {
	switch e {
	default:
		panic("bug")

	case 'Z':
		// pickle: str|bytes
		v, err := pickletools.Xstrbytes8(xv)
		if err != nil {
			return 0, false
		}
		return v, true
	}
}

// xuint64Pack packs v into big-endian 8-byte string
func (e encoding) xuint64Pack(v uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)

	switch e {
	default:
		panic("bug")

	case 'Z':
		// pickle: -> str	XXX do we need to emit bytes instead of str?
		return mem.String(b[:])
	}
}

// Tid converts tid into corresponding object appropriate for encoding e.
func (e encoding) Tid(tid zodb.Tid) string {
	return e.xuint64Pack(uint64(tid))
}

// Oid converts oid into corresponding object appropriate for encoding e.
func (e encoding) Oid(oid zodb.Oid) string {
	return e.xuint64Pack(uint64(oid))
}

// asTid tries to retrieve Tid from corresponding object decoded via encoding e.
func (e encoding) asTid(xv interface{}) (zodb.Tid, bool) {
	v, ok := e.xuint64Unpack(xv)
	return zodb.Tid(v), ok
}

// asOid tries to retrieve Oid from corresponding object decoded via encoding e.
func (e encoding) asOid(xv interface{}) (zodb.Oid, bool) {
	v, ok := e.xuint64Unpack(xv)
	return zodb.Oid(v), ok
}


// asBytes tries to retrieve bytes from corresponding object decoded via encoding e.
func (e encoding) asBytes(xb interface{}) ([]byte, bool) {
	switch e {
	default:
		panic("bug")

	case 'Z':
		// pickle: str|bytes
		s, err := pickletools.Xstrbytes(xb)
		if err != nil {
			return nil, false
		}
		return mem.Bytes(s), true
	}
}

// asString tries to retrieve string from corresponding object decoded via encoding e.
func (e encoding) asString(xs interface{}) (string, bool) {
	switch e {
	default:
		panic("bug")

	case 'Z':
		// pickle: str
		s, ok := xs.(string)
		return s, ok
	}
}
