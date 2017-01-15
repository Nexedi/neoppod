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
	"reflect"
	"testing"
	"unsafe"
)

func TestPktHeader(t *testing.T) {
	// make sure PktHeader is really packed
	if unsafe.Sizeof(PktHead{}) != 10 {
		t.Fatalf("sizeof(PktHead) = %v  ; want 10", unsafe.Sizeof(PktHead{}))
	}
}

// test encoding/decoding of packets
func TestPktMarshal(t *testing.T) {
	var testv = []struct {
		pkt     NEODecoder	//interface {NEOEncoder; NEODecoder}
		encoded string	// []byte
	} {
		{&Ping{}, ""},
		{&Error{Code: 0x01020304, Message: "hello"}, "\x01\x02\x03\x04\x00\x00\x00\x05hello"},
	}

	for _, tt := range testv {
		// TODO check encoding

		// check decoding
		data := tt.encoded + "noise"
		typ := reflect.TypeOf(tt.pkt).Elem()	// type of *pkt
		pkt2 := reflect.New(typ).Interface().(NEODecoder)
		n, err := pkt2.NEODecode([]byte(data))	// XXX
		if err != nil {
			t.Errorf("%v: decode error %v", typ, err)
		}
		if n != len(tt.encoded) {
			t.Errorf("%v: nread = %v  ; want %v", typ, n, len(tt.encoded))
		}

		if !reflect.DeepEqual(pkt2, tt.pkt) {
			t.Errorf("%v: decode result unexpected: %v  ; want %v", typ, pkt2, tt.pkt)
		}

		// decode must overflow on cut data
		for l := len(tt.encoded)-1; l >= 0; l-- {
			data = tt.encoded[:l]	// XXX also check on original byte [:l] ?
			n, err = pkt2.NEODecode([]byte(data))	// XXX
			if !(n==0 && err==ErrDecodeOverflow) {
				t.Errorf("%v: decode overflow not detected on [:%v]", typ, l)
			}

		}
	}
}
