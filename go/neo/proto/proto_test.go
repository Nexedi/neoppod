// Copyright (C) 2016-2020  Nexedi SA and Contributors.
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

package proto
// NEO. protocol encoding tests

import (
	"encoding/binary"
	hexpkg "encoding/hex"
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

// uint8 -> string as encoded on the wire
func u8(v uint8) string {
	return string(v)
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
	// make sure PktHeader is really packed and its size matches PktHeaderLen
	if unsafe.Sizeof(PktHeader{}) != 10 {
		t.Fatalf("sizeof(PktHeader) = %v  ; want 10", unsafe.Sizeof(PktHeader{}))
	}
	if unsafe.Sizeof(PktHeader{}) != PktHeaderLen {
		t.Fatalf("sizeof(PktHeader) = %v  ; want %v", unsafe.Sizeof(PktHeader{}), PktHeaderLen)
	}
}

// test marshalling for one message type
func testMsgMarshal(t *testing.T, msg Msg, encoded string) {
	typ := reflect.TypeOf(msg).Elem() // type of *msg
	msg2 := reflect.New(typ).Interface().(Msg)
	defer func() {
		if e := recover(); e != nil {
			t.Errorf("%v: panic ↓↓↓:", typ)
			panic(e) // to show traceback
		}
	}()

	// msg.encode() == expected
	msgCode := msg.NEOMsgCode()
	n := msg.NEOMsgEncodedLen()
	msgType := MsgType(msgCode)
	if msgType != typ {
		t.Errorf("%v: msgCode = %v  which corresponds to %v", typ, msgCode, msgType)
	}
	if n != len(encoded) {
		t.Errorf("%v: encodedLen = %v  ; want %v", typ, n, len(encoded))
	}

	buf := make([]byte, n)
	msg.NEOMsgEncode(buf)
	if string(buf) != encoded {
		t.Errorf("%v: encode result unexpected:", typ)
		t.Errorf("\thave: %s", hexpkg.EncodeToString(buf))
		t.Errorf("\twant: %s", hexpkg.EncodeToString([]byte(encoded)))
	}

	// encode must panic if passed a smaller buffer
	for l := len(buf) - 1; l >= 0; l-- {
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
				if !(strings.Contains(estr, "slice bounds out of range") ||
				     strings.Contains(estr, "index out of range")) {
					t.Errorf("%s unexpected runtime panic: %v", subj, estr)
				}
			}()

			msg.NEOMsgEncode(buf[:l])
		}()
	}

	// msg.decode() == expected
	data := []byte(encoded + "noise")
	n, err := msg2.NEOMsgDecode(data)
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
	for l := len(encoded) - 1; l >= 0; l-- {
		n, err = msg2.NEOMsgDecode(data[:l])
		if !(n == 0 && err == ErrDecodeOverflow) {
			t.Errorf("%v: decode overflow not detected on [:%v]", typ, l)
		}

	}
}

// test encoding/decoding of messages
func TestMsgMarshal(t *testing.T) {
	var testv = []struct {
		msg     Msg
		encoded string // []byte
	}{
		// empty
		{&Ping{}, ""},

		// uint8, string
		{&Error{Code: 0x04, Message: "hello"}, "\x04\x00\x00\x00\x05hello"},

		// Oid, Tid, bool, Checksum, []byte
		{&StoreObject{
			Oid:         0x0102030405060708,
			Serial:      0x0a0b0c0d0e0f0102,
			Compression: false,
			Checksum:    Checksum{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}, // XXX simpler?
			Data:        []byte("hello world"),
			DataSerial:  0x0a0b0c0d0e0f0103,
			Tid:         0x0a0b0c0d0e0f0104,
			},

			hex("01020304050607080a0b0c0d0e0f010200") +
			hex("0102030405060708090a0b0c0d0e0f1011121314") +
			hex("0000000b") + "hello world" +
			hex("0a0b0c0d0e0f01030a0b0c0d0e0f0104")},

		// PTid, [] (of [] of {UUID, CellState})
		{&AnswerPartitionTable{
			PTid:    0x0102030405060708,
			RowList: []RowInfo{
				{1, []CellInfo{{11, UP_TO_DATE}, {17, OUT_OF_DATE}}},
				{2, []CellInfo{{11, FEEDING}}},
				{7, []CellInfo{{11, CORRUPTED}, {15, DISCARDED}, {23, UP_TO_DATE}}},
			},
			},

			hex("0102030405060708") +
			hex("00000003") +
				hex("00000001000000020000000b010000001100") +
				hex("00000002000000010000000b02") +
				hex("00000007000000030000000b030000000f040000001701"),
		},

		// map[Oid]struct {Tid,Tid,bool}
		{&AnswerObjectUndoSerial{
			ObjectTIDDict: map[zodb.Oid]struct {
				CurrentSerial zodb.Tid
				UndoSerial    zodb.Tid
				IsCurrent     bool
			}{
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
		{&PartitionCorrupted{7, []NodeUUID{1, 3, 9, 4}},
			u32(7) + u32(4) + u32(1) + u32(3) + u32(9) + u32(4),
		},

		// uint32, Address, string, IdTime
		{&RequestIdentification{CLIENT, 17, Address{"localhost", 7777}, "myname", 0.12345678},

			u8(2) + u32(17) + u32(9) +
			"localhost" + u16(7777) +
			u32(6) + "myname" +
			hex("3fbf9add1091c895"),
		},

		// IdTime, empty Address, int32
		{&NotifyNodeInformation{1504466245.926185, []NodeInfo{
			{CLIENT, Address{}, UUID(CLIENT, 1), RUNNING, 1504466245.925599}}},

			hex("41d66b15517b469d") + u32(1) +
				u8(2) + u32(0) /* <- ø Address */ + hex("e0000001") + u8(2) +
				hex("41d66b15517b3d04"),
		},

		// empty IdTime
		{&NotifyNodeInformation{IdTimeNone, []NodeInfo{}}, hex("ffffffffffffffff") + hex("00000000")},

		// TODO we need tests for:
		// []varsize + trailing
		// map[]varsize + trailing
	}

	for _, tt := range testv {
		testMsgMarshal(t, tt.msg, tt.encoded)
	}
}

// For all message types: same as testMsgMarshal but zero-values only.
// this way we additionally lightly check encode / decode overflow behaviour for all types.
func TestMsgMarshalAllOverflowLightly(t *testing.T) {
	for _, typ := range msgTypeRegistry {
		// zero-value for a type
		msg := reflect.New(typ).Interface().(Msg)
		l := msg.NEOMsgEncodedLen()
		zerol := make([]byte, l)
		// decoding will turn nil slice & map into empty allocated ones.
		// we need it so that reflect.DeepEqual works for msg encode/decode comparison
		n, err := msg.NEOMsgDecode(zerol)
		if !(n == l && err == nil) {
			t.Errorf("%v: zero-decode unexpected: %v, %v  ; want %v, nil", typ, n, err, l)
		}

		testMsgMarshal(t, msg, string(zerol))
	}
}

// Verify overflow handling on decode len checks
func TestMsgDecodeLenOverflow(t *testing.T) {
	var testv = []struct {
		msg  Msg    // of type to decode into
		data string // []byte - tricky data to exercise decoder u32 len checks overflow
	}{
		// [] with sizeof(item) = 8	-> len*sizeof(item) = 0 if u32
		{&AnswerTIDs{},               u32(0x20000000)},

		// {} with sizeof(key) = 8, sizeof(value) = 8	-> len*sizeof(key+value) = 0 if u32
		{&AnswerLockedTransactions{}, u32(0x10000000)},
	}

	for _, tt := range testv {
		data := []byte(tt.data)
		func() {
			defer func() {
				if e := recover(); e != nil {
					t.Errorf("%T: decode: panic on %x", tt.msg, data)
				}
			}()

			n, err := tt.msg.NEOMsgDecode(data)
			if !(n == 0 && err == ErrDecodeOverflow) {
				t.Errorf("%T: decode %x\nhave: %d, %v\nwant: %d, %v", tt.msg, data,
					n, err, 0, ErrDecodeOverflow)
			}
		}()
	}
}

func TestUUID(t *testing.T) {
	var testv = []struct{typ NodeType; num int32; uuid uint32; str string}{
		{STORAGE, 1, 0x00000001, "S1"},
		{MASTER,  2, 0xf0000002, "M2"},
		{CLIENT,  3, 0xe0000003, "C3"},
		{ADMIN,   4, 0xd0000004, "A4"},
	}

	for _, tt := range testv {
		uuid := UUID(tt.typ, tt.num)
		if uint32(uuid) != tt.uuid {
			t.Errorf("%v: uuid=%08x  ; want %08x", tt, uuid, tt.uuid)
		}
		if uuids := uuid.String(); uuids != tt.str {
			t.Errorf("%v: str(uuid): %q  ; want %q", tt, uuids, tt.str)
		}
	}
}

func TestUUIDDecode(t *testing.T) {
	var testv = []struct{uuid uint32; str string}{
		{0,          "?(0)0"},
		{0x00000001, "S1"},
		{0xf0000002, "M2"},
		{0xe0000003, "C3"},
		{0xd0000004, "A4"},
		{0xc0000005, "?(4)5"},
		{0xb0000006, "?(5)6"},
		{0xa0000007, "?(6)7"},
		{0x90000008, "?(7)8"},
		{0x80000009, "?(8)9"},
		{0x7000000a, "?(9)10"},
		{0x6000000b, "?(10)11"},
		{0x5000000c, "?(11)12"},
		{0x4000000d, "?(12)13"},
		{0x3000000e, "?(13)14"},
		{0x2000000f, "?(14)15"},
		{0x10000010, "?(15)16"},
		{0x00000011, "S17"},
	}

	for _, tt := range testv {
		str := NodeUUID(tt.uuid).String()
		if str != tt.str {
			t.Errorf("%08x -> %q  ; want %q", tt.uuid, str, tt.str)
		}
	}
}
