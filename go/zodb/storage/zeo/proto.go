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

// ---- message encode/decode ↔ packet ----

// pktDecode decodes raw packet into message.
func pktDecode(pkb *pktBuf) (msg, error) {
	var m msg
	// must be (msgid, False|0, ".reply", res)
	d := pickle.NewDecoder(bytes.NewReader(pkb.Payload()))
	xpkt, err := d.Decode()
	if err != nil {
		return m, err
	}

	tpkt, ok := xpkt.(pickle.Tuple) // XXX also list?
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
		return m, derrf("flags: got %T; expected int", tpkt[1])
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


// ---- oid/tid packing ----

// xuint64Unpack tries to decode packed 8-byte string as bigendian uint64
func xuint64Unpack(xv interface{}) (uint64, bool) {
	v, err := pickletools.Xstrbytes8(xv)
	if err != nil {
		return 0, false
	}

	return v, true
}

// xuint64Pack packs v into big-endian 8-byte string
//
// XXX do we need to emit bytes instead of str?
func xuint64Pack(v uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return mem.String(b[:])
}

func tidPack(tid zodb.Tid) string {
	return xuint64Pack(uint64(tid))
}

func oidPack(oid zodb.Oid) string {
	return xuint64Pack(uint64(oid))
}

func tidUnpack(xv interface{}) (zodb.Tid, bool) {
	v, ok := xuint64Unpack(xv)
	return zodb.Tid(v), ok
}

func oidUnpack(xv interface{}) (zodb.Oid, bool) {
	v, ok := xuint64Unpack(xv)
	return zodb.Oid(v), ok
}
