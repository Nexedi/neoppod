// Copyright (C) 2016  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 2, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

// NEO. Protocol definition. Tests

package neo

import (
	hexpkg "encoding/hex"
	"encoding/binary"
	"reflect"
	"testing"
	"unsafe"
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
	// make sure PktHeader is really packed
	if unsafe.Sizeof(PktHead{}) != 10 {
		t.Fatalf("sizeof(PktHead) = %v  ; want 10", unsafe.Sizeof(PktHead{}))
	}
}

// XXX move me out of here?
type NEOCodec interface {
	NEOEncoder
	NEODecoder
}

// test marshalling for one packet type
func testPktMarshal(t *testing.T, pkt NEOCodec, encoded string) {
	typ := reflect.TypeOf(pkt).Elem()	// type of *pkt
	pkt2 := reflect.New(typ).Interface().(NEOCodec)
	defer func() {
		if e := recover(); e != nil {
			t.Errorf("%v: panic ↓↓↓:", typ)
			panic(e)	// to show traceback
		}
	}()

	// check encoding
	n := pkt.NEOEncodedLen()
	if n != len(encoded) {
		t.Errorf("%v: encodedLen = %v  ; want %v", typ, n, len(encoded))
	}

	buf := make([]byte, n)
	pkt.NEOEncode(buf)
	if string(buf) != encoded {
		t.Errorf("%v: encode result unexpected:", typ)
		t.Errorf("\thave: %s", hexpkg.EncodeToString(buf))
		t.Errorf("\twant: %s", hexpkg.EncodeToString([]byte(encoded)))
	}

	// TODO encode - check == encoded
	// TODO encode(smaller buf) -> panic

	// check decoding
	data := encoded + "noise"
	n, err := pkt2.NEODecode([]byte(data))	// XXX
	if err != nil {
		t.Errorf("%v: decode error %v", typ, err)
	}
	if n != len(encoded) {
		t.Errorf("%v: nread = %v  ; want %v", typ, n, len(encoded))
	}

	if !reflect.DeepEqual(pkt2, pkt) {
		t.Errorf("%v: decode result unexpected: %v  ; want %v", typ, pkt2, pkt)
	}

	// decode must overflow on cut data	TODO reenable
	/*
	for l := len(encoded)-1; l >= 0; l-- {
		// NOTE cap must not be > l
		data = encoded[:l]	// XXX also check on original byte [:l] ?
		n, err = pkt2.NEODecode([]byte(data))	// XXX
		if !(n==0 && err==ErrDecodeOverflow) {
			t.Errorf("%v: decode overflow not detected on [:%v]", typ, l)
		}

	}
	*/
}

// test encoding/decoding of packets
func TestPktMarshal(t *testing.T) {
	var testv = []struct {
		pkt     NEOCodec
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
			Unlock:	true,
			},

		 hex("01020304050607080a0b0c0d0e0f010200") +
		 hex("0102030405060708090a0b0c0d0e0f1011121314") +
		 hex("0000000b") + "hello world" +
		 hex("0a0b0c0d0e0f01030a0b0c0d0e0f010401")},

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

		/*
		// map[Oid]struct {Tid,Tid,bool}
		{&AnswerObjectUndoSerial{
			ObjectTIDDict: map[Oid]struct{
						CurrentSerial   Tid
						UndoSerial      Tid
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
			u64(8) + u64(7) + u64(1) + hex("00") +
			u64(5) + u64(4) + u64(3) + hex("01"),
		},
		*/

		// uint32, []uint32
		{&PartitionCorrupted{7, []UUID{1,3,9,4}},
		 u32(7) + u32(4) + u32(1) + u32(3) + u32(9) + u32(4),
		},

		// uint32, Address, string, float64
		{&RequestIdentification{8, CLIENT, 17, Address{"localhost", 7777}, "myname", 0.12345678},

		 u32(8) + u32(2) + u32(17) + u32(9) +
		 "localhost" + u16(7777) +
		 u32(6) + "myname" +
		 hex("3fbf9add1091c895"),
		},


		// TODO special cases for:
		// - float64 (+ nan !nan ...)
		// - Address,
	}

	for _, tt := range testv {
		testPktMarshal(t, tt.pkt, tt.encoded)
	}
}
