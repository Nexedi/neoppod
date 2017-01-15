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

func TestPktHeader(t *testing.T) {
	// make sure PktHeader is really packed
	if unsafe.Sizeof(PktHead{}) != 10 {
		t.Fatalf("sizeof(PktHead) = %v  ; want 10", unsafe.Sizeof(PktHead{}))
	}
}

func testPktMarshal(t *testing.T, pkt NEODecoder, encoded string) {
	typ := reflect.TypeOf(pkt).Elem()	// type of *pkt
	pkt2 := reflect.New(typ).Interface().(NEODecoder)
	defer func() {
		if e := recover(); e != nil {
			t.Errorf("%v: panic ↓↓↓:", typ)
			panic(e)	// to show traceback
		}
	}()

	// TODO check encoding

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
		pkt     NEODecoder	//interface {NEOEncoder; NEODecoder}
		encoded string	// []byte
	} {
		// empty
		{&Ping{}, ""},

		// uint32, string	XXX string -> Notify?
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

		// PTid, [] (of [] of ...)
		{&AnswerPartitionTable{
			PTid:	0x0102030405060708,
			RowList: []RowInfo{
				{1, []CellInfo{{11, UP_TO_DATE}, {17, OUT_OF_DATE}}},
				{2, []CellInfo{{11, FEEDING}}},
				{7, []CellInfo{{11, CORRUPTED}, {15, DISCARDED}, {23, UP_TO_DATE}}},
			},
		 },

		 hex("")},

		/*
		// uint32, Address, string, float64
		{&RequestIdentification{...}},	// TODO
		*/

		// TODO float64 (+ nan !nan ...)
		// TODO [](!byte), map
		// TODO Address, PTid
	}

	for _, tt := range testv {
		testPktMarshal(t, tt.pkt, tt.encoded)
	}
}
