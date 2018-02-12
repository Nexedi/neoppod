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

// Package packed provides types to use in packed structures.
package packed

// uintX has alignment requirement =X; [X]byte has alignment requirement 1.
// That's why we can use [X]byte and this way keep a struct packed, even if Go
// does not support packed structs in general.
//
// XXX SSA backend does not handle arrays well - it handles structs better -
// e.g. there are unnecessary clears in array case:
//	https://github.com/golang/go/issues/15925
//
// so in the end we use hand-crafted array-like byte-structs.
type BE16 struct { _0, _1 byte }
type BE32 struct { _0, _1, _2, _3 byte }


func Ntoh16(v BE16) uint16 {
	// XXX not as good as BigEndian.Uint16
	// (unnecessary MOVBLZX AL, AX + shifts not combined into ROLW $8)
	return  uint16(v._1) | uint16(v._0)<<8
}

func Hton16(v uint16) BE16 {
	return BE16{byte(v>>8), byte(v)}
}

func Ntoh32(v BE32) uint32 {
	// XXX not as good as BigEndian.Uint32
	// (unnecessary MOVBLZX AL, AX + shifts not combined into BSWAPL)
	return  uint32(v._3) | uint32(v._2)<<8 | uint32(v._1)<<16 | uint32(v._0)<<24
}

func Hton32(v uint32) BE32 {
	return BE32{byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}
}
