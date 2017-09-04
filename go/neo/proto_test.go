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
// protocol tests

import (
	hexpkg "encoding/hex"
	"encoding/binary"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"lab.nexedi.com/kirr/neo/go/zodb"
)

// decode string as hex; panic on error
func hex(s string) string {
	b, err := hexpkg.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// uint16 -> string as encoded on the wire
func u16(v uint16) string {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return string(b[:])
}

// uint32 -> string as encoded on the wire
func u32(v uint32) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return string(b[:])
}

// uint64 -> string as encoded on the wire
func u64(v uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return string(b[:])
}

func TestPktHeader(t *testing.T) {
	// make sure PktHeader is really packed and its size matches pktHeaderLen
	if unsafe.Sizeof(PktHeader{}) != 10 {
		t.Fatalf("sizeof(PktHeader) = %v  ; want 10", unsafe.Sizeof(PktHeader{}))
	}
	if unsafe.Sizeof(PktHeader{}) != pktHeaderLen {
		t.Fatalf("sizeof(PktHeader) = %v  ; want %v", unsafe.Sizeof(PktHeader{}), pktHeaderLen)
	}
}

// test marshalling for one message type
func testMsgMarshal(t *testing.T, msg Msg, encoded string) {
	typ := reflect.TypeOf(msg).Elem()	// type of *msg
	msg2 := reflect.New(typ).Interface().(Msg)
	defer func() {
		if e := recover(); e != nil {
			t.Errorf("%v: panic ↓↓↓:", typ)
			panic(e)	// to show traceback
		}
	}()

	// msg.encode() == expected
	msgCode := msg.neoMsgCode()
	n := msg.neoMsgEncodedLen()
	msgType := msgTypeRegistry[msgCode]
	if msgType != typ {
		t.Errorf("%v: msgCode = %v  which corresponds to %v", typ, msgCode, msgType)
	}
	if n != len(encoded) {
		t.Errorf("%v: encodedLen = %v  ; want %v", typ, n, len(encoded))
	}

	buf := make([]byte, n)
	msg.neoMsgEncode(buf)
	if string(buf) != encoded {
		t.Errorf("%v: encode result unexpected:", typ)
		t.Errorf("\thave: %s", hexpkg.EncodeToString(buf))
		t.Errorf("\twant: %s", hexpkg.EncodeToString([]byte(encoded)))
	}

	// encode must panic if passed a smaller buffer
	for l := len(buf)-1; l >= 0; l-- {
		func() {
			defer func() {
				subj := fmt.Sprintf("%v: encode(buf[:encodedLen-%v])", typ, len(encoded)-l)
				e := recover()
				if e == nil {
					t.Errorf("%s did not panic", subj)
					return
				}

				err, ok := e.(runtime.Error)
				if !ok {
					t.Errorf("%s panic(%#v)  ; want runtime.Error", subj, e)
				}

				estr := err.Error()
				if ! (strings.Contains(estr, "slice bounds out of range") ||
				      strings.Contains(estr, "index out of range")) {
				      t.Errorf("%s unexpected runtime panic: %v", subj, estr)
				}
			}()

			msg.neoMsgEncode(buf[:l])
		}()
	}

	// msg.decode() == expected
	data := []byte(encoded + "noise")
	n, err := msg2.neoMsgDecode(data)
	if err != nil {
		t.Errorf("%v: decode error %v", typ, err)
	}
	if n != len(encoded) {
		t.Errorf("%v: nread = %v  ; want %v", typ, n, len(encoded))
	}

	if !reflect.DeepEqual(msg2, msg) {
		t.Errorf("%v: decode result unexpected: %v  ; want %v", typ, msg2, msg)
	}

	// decode must detect buffer overflow
	for l := len(encoded)-1; l >= 0; l-- {
		n, err = msg2.neoMsgDecode(data[:l])
		if !(n==0 && err==ErrDecodeOverflow) {
			t.Errorf("%v: decode overflow not detected on [:%v]", typ, l)
		}

	}
}

// test encoding/decoding of messages
func TestMsgMarshal(t *testing.T) {
	var testv = []struct {
		msg     Msg
		encoded string	// []byte
	} {
		// empty
		{&Ping{}, ""},

		// uint32, string
		{&Error{Code: 0x01020304, Message: "hello"}, "\x01\x02\x03\x04\x00\x00\x00\x05hello"},

		// Oid, Tid, bool, Checksum, []byte
		{&StoreObject{
			Oid:	0x0102030405060708,
			Serial: 0x0a0b0c0d0e0f0102,
			Compression: false,
			Checksum: Checksum{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20},	// XXX simpler?
			Data:	[]byte("hello world"),
			DataSerial: 0x0a0b0c0d0e0f0103,
			Tid:	0x0a0b0c0d0e0f0104,
			},

		 hex("01020304050607080a0b0c0d0e0f010200") +
		 hex("0102030405060708090a0b0c0d0e0f1011121314") +
		 hex("0000000b") + "hello world" +
		 hex("0a0b0c0d0e0f01030a0b0c0d0e0f0104")},

		// PTid, [] (of [] of {UUID, CellState})
		{&AnswerPartitionTable{
			PTid:	0x0102030405060708,
			RowList: []RowInfo{
				{1, []CellInfo{{11, UP_TO_DATE}, {17, OUT_OF_DATE}}},
				{2, []CellInfo{{11, FEEDING}}},
				{7, []CellInfo{{11, CORRUPTED}, {15, DISCARDED}, {23, UP_TO_DATE}}},
			},
		 },

		 hex("0102030405060708") +
		 hex("00000003") +
			hex("00000001000000020000000b000000000000001100000001") +
			hex("00000002000000010000000b00000002") +
			hex("00000007000000030000000b000000040000000f000000030000001700000000"),
		},

		// map[Oid]struct {Tid,Tid,bool}
		{&AnswerObjectUndoSerial{
			ObjectTIDDict: map[zodb.Oid]struct{
						CurrentSerial   zodb.Tid
						UndoSerial      zodb.Tid
						IsCurrent       bool
				} {
				1: {1, 0, false},
				2: {7, 1, true},
				8: {7, 1, false},
				5: {4, 3, true},
			}},

		 u32(4) +
			u64(1) + u64(1) + u64(0) + hex("00") +
			u64(2) + u64(7) + u64(1) + hex("01") +
			u64(5) + u64(4) + u64(3) + hex("01") +
			u64(8) + u64(7) + u64(1) + hex("00"),
		},

		// map[uint32]UUID + trailing ...
		{&CheckReplicas{
			PartitionDict: map[uint32]NodeUUID{
				1: 7,
				2: 9,
				7: 3,
				4: 17,
			},
			MinTID: 23,
			MaxTID: 128,
			},

		 u32(4) +
			u32(1) + u32(7) +
			u32(2) + u32(9) +
			u32(4) + u32(17) +
			u32(7) + u32(3) +
		 u64(23) + u64(128),
		},

		// uint32, []uint32
		{&PartitionCorrupted{7, []NodeUUID{1,3,9,4}},
		 u32(7) + u32(4) + u32(1) + u32(3) + u32(9) + u32(4),
		},

		// uint32, Address, string, float64
		{&RequestIdentification{CLIENT, 17, Address{"localhost", 7777}, "myname", 0.12345678},

		 u32(2) + u32(17) + u32(9) +
		 "localhost" + u16(7777) +
		 u32(6) + "myname" +
		 hex("3fbf9add1091c895"),
		},

		// float64, empty Address, int32
		{&NotifyNodeInformation{1504466245.926185, []NodeInfo{
			{CLIENT, Address{}, UUID(CLIENT, 1), RUNNING, 1504466245.925599}}},

		 hex("41d66b15517b469d") + u32(1) +
			u32(2) + u32(0) /* <- ø Address */ + hex("e0000001") + u32(2) +
			hex("41d66b15517b3d04"),
		},

		// TODO we need tests for:
		// []varsize + trailing
		// map[]varsize + trailing


		// TODO special cases for:
		// - float64 (+ nan !nan ...)
	}

	for _, tt := range testv {
		testMsgMarshal(t, tt.msg, tt.encoded)
	}
}

// For all message types: same as testMsgMarshal but zero-values only
// this way we additionally lightly check encode / decode overflow behaviour for all types.
func TestMsgMarshalAllOverflowLightly(t *testing.T) {
	for _, typ := range msgTypeRegistry {
		// zero-value for a type
		msg := reflect.New(typ).Interface().(Msg)
		l := msg.neoMsgEncodedLen()
		zerol := make([]byte, l)
		// decoding will turn nil slice & map into empty allocated ones.
		// we need it so that reflect.DeepEqual works for msg encode/decode comparison
		n, err := msg.neoMsgDecode(zerol)
		if !(n == l && err == nil) {
			t.Errorf("%v: zero-decode unexpected: %v, %v  ; want %v, nil", typ, n, err, l)
		}

		testMsgMarshal(t, msg, string(zerol))
	}
}
