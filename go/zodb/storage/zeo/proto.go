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
// Protocol for exchanging ZEO messages.
// On the wire messages are encoded via either pickles or msgpack.
// Each message is wrapped into packet with be32 header of whole packet size.
// See https://github.com/zopefoundation/ZEO/blob/5.2.1-20-gcb26281d/doc/protocol.rst for details.

import (
	"bytes"
	"encoding/binary"
	"fmt"

	msgp    "github.com/tinylib/msgp/msgp"
	msgpack "github.com/shamaton/msgpack"
	pickle  "github.com/kisielk/og-rek"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/internal/pickletools"
)

// msg represents 1 message.
// arg is arbitrary argument(s) passed/received along ZEO call or reply.
//
// for objects in arg user code has to be obtain them via encoding.as*() and
// set them via encoding.Tid(), encoding.Oid() and other similar methods that
// convert application-level data into objects properly corresponding to wire
// encoding of messages.
type msg struct {
	msgid  int64
	flags  msgFlags
	method string
	arg    interface{} // can be e.g. tuple(arg1, arg2, ...)
}

type msgFlags int64
const (
	msgAsync  msgFlags = 1 // message does not need a reply
	msgExcept          = 2 // exception was raised on remote side (ZEO5)
)

// encoding represents messages encoding.
type encoding byte // Z - pickles, M - msgpack

// ---- message encode/decode ↔ packet ----

// pktEncode encodes message into raw packet.
func (e encoding) pktEncode(m msg) *pktBuf {
	switch e {
	case 'Z': return pktEncodeZ(m)
	case 'M': return pktEncodeM(m)
	default:  panic("bug")
	}
}

// pktDecode decodes raw packet into message.
func (e encoding) pktDecode(pkb *pktBuf) (msg, error) {
	switch e {
	case 'Z': return pktDecodeZ(pkb)
	case 'M': return pktDecodeM(pkb)
	default:  panic("bug")
	}
}


// pktEncodeZ encodes message into raw Z (pickle) packet.
func pktEncodeZ(m msg) *pktBuf {
	pkb := allocPkb()
	p := pickle.NewEncoder(pkb)

/*
	// tuple -> pickle.Tuple	XXX needed? (should be produced by enc.Tuple)
	arg := m.arg
	tup, ok := arg.(tuple)
	if ok {
		arg = pickle.Tuple(tup)
	}
*/
	err := p.Encode(pickle.Tuple{m.msgid, m.flags, m.method, m.arg})
	if err != nil {
		panic(err) // all our types are expected to be supported by pickle
	}
	return pkb
}

// pktEncodeM encodes message into raw M (msgpack) packet.
func pktEncodeM(m msg) *pktBuf {
	pkb := allocPkb()

	data := pkb.data
	data = msgp.AppendArrayHeader(data, 4)
	data = msgp.AppendInt64(data,  m.msgid)		// msgid
	data = msgp.AppendInt64(data,  int64(m.flags))	// flags
	data = msgp.AppendString(data, m.method)	// method
	// arg
	// it is interface{} - use shamaton/msgpack since msgp does not handle
	// arbitrary interfaces well.
/*
	// XXX shamaton/msgpack encodes tuple(nil) as nil, not empty tuple
	// XXX move to zLink.Call?
	arg := m.arg
	tup, ok := arg.(tuple)
	if ok && tup == nil {
		arg = tuple{}
	}
	dataArg, err := msgpack.Encode(arg)
*/
	dataArg, err := msgpack.Encode(m.arg)
	if err != nil {
		panic(err) // all our types are expected to be supported by msgpack
	}
	data = append(data, dataArg...)

	pkb.data = data
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

	tpkt, ok := xpkt.(pickle.Tuple) // XXX also list? -> Z.asTuple(xpkt)
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

// pktDecodeM decodes raw M (msgpack) packet into message.
func pktDecodeM(pkb *pktBuf) (msg, error) {
	var m msg
	b := pkb.Payload()

	// must be (msgid, False|0, "method", arg)
	l, b, err := msgp.ReadArrayHeaderBytes(b)
	if err != nil {
		return m, derrf("%s", err)
	}
	if l != 4 {
		return m, derrf("len(msg-tuple)=%d; expected 4", l)
	}

	// msgid
	v := int64(0)
	switch t := msgp.NextType(b); t {
	case msgp.IntType:
		v, b, err = msgp.ReadInt64Bytes(b)
	case msgp.UintType:
		var x uint64
		x, b, err = msgp.ReadUint64Bytes(b)
		v = int64(x)
	default:
		err = fmt.Errorf("got %s; expected int", t)
	}
	if err != nil {
		return m, derrf("msgid: %s", err)
	}
	m.msgid = v

	// flags
	v = int64(0)
	switch t := msgp.NextType(b); t {
	case msgp.BoolType:
		var x bool
		x, b, err = msgp.ReadBoolBytes(b)
		if x { v = 1 }
	case msgp.IntType:
		v, b, err = msgp.ReadInt64Bytes(b)
	case msgp.UintType:
		var x uint64
		x, b, err = msgp.ReadUint64Bytes(b)
		v = int64(x)
	default:
		err = fmt.Errorf("got %s; expected int|bool", t)
	}
	if err != nil {
		return m, derrf("flags: %s", err)
	}
	// XXX check flags are in range?
	m.flags = msgFlags(v)

	// method
	s := ""
	switch t := msgp.NextType(b); t {
	case msgp.StrType:
		s, b, err = msgp.ReadStringBytes(b)
	case msgp.BinType:
		var x []byte
		x, b, err = msgp.ReadBytesZC(b)
		s = string(x)
	default:
		err = fmt.Errorf("got %s; expected str|bin", t)
	}
	if err != nil {
		return m, derrf(".%d: method: %s", m.msgid, err)
	}
	m.method = s

	// arg
	// it is interface{} - use shamaton/msgpack since msgp does not handle
	// arbitrary interfaces well.
	btail, err := msgp.Skip(b)
	if err != nil {
		return m, derrf(".%d: arg: %s", m.msgid, err)
	}
	if len(btail) != 0 {
		return m, derrf(".%d: payload has extra data after message")
	}
	err = msgpack.Decode(b, &m.arg)
	if err != nil {
		return m, derrf(".%d: arg: %s", m.msgid, err)
	}

	return m, nil
}


func derrf(format string, argv ...interface{}) error {
	return fmt.Errorf("decode: "+format, argv...)
}


// ---- retrieve/put objects from/into msg.arg ----

// xuint64Unpack tries to retrieve packed 8-byte string as bigendian uint64.
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

	case 'M':
		// msgpack decodes bytes as []byte (which corresponds to bytearray in pickle)
		switch v := xv.(type) {
		default:
			return 0, false

		case []byte:
			if len(v) != 8 {
				return 0, false
			}
			return binary.BigEndian.Uint64(v), true
		}
	}

}

// xuint64Pack packs v into big-endian 8-byte string.
func (e encoding) xuint64Pack(v uint64) interface{} {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)

	switch e {
	default:
		panic("bug")

	case 'Z':
		// pickle: -> str	XXX do we need to emit bytes for py3?  -> TODO yes, after switch to protocol=3
		return mem.String(b[:])

	case 'M':
		// msgpack: -> bin
		return b[:]
	}
}

// Tid converts tid into corresponding object appropriate for encoding e.
func (e encoding) Tid(tid zodb.Tid) interface{} {
	return e.xuint64Pack(uint64(tid))
}

// Oid converts oid into corresponding object appropriate for encoding e.
func (e encoding) Oid(oid zodb.Oid) interface{} {
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


// tuple represents py tuple.
type tuple []interface{}

// Tuple converts t into corresponding object appropriate for encoding e.
func (e encoding) Tuple(t tuple) interface{} {
	switch e {
	default:
		panic("bug")

	case 'Z':
		// pickle: -> pickle.Tuple
		return pickle.Tuple(t)

	case 'M':
		// msgpack: -> leave as tuple
		// However shamaton/msgpack encodes tuple(nil) as nil, not empty tuple
		// so nil -> tuple{}
		if t == nil {
			t = tuple{}
		}
		return t
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

	case 'M':
		// msgpack: tuples/lists are encoded as arrays; decoded as []interface{}
		t, ok := xt.([]interface{})
		return tuple(t), ok
	}
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

	case 'M':
		// msgpack: bin
		b, ok := xb.([]byte)
		return b, ok
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

	case 'M':
		// msgpack: bin(from py2) | str(from py3)
		switch s := xs.(type) {
		case []byte:
			return string(s), true
		case string:
			return s, true
		default:
			return "", false
		}
	}
}
