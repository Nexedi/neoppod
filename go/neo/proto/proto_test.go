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

package proto

import (
	"testing"
)

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
