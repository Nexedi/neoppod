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

// Types to use in packed structures

package neo

import (
	"encoding/binary"
	"unsafe"
)

// uintX has alignment requirement =X; [X]byte has alignment requirement 1.
// That's why we can use [X]byte and this way keep a struct packed, even if Go
// does not support packed structs in general.
//
// XXX SSA backend does not handle arrays well - it handles structs better -
// e.g. there are unnecessary clears in array case:
//	https://github.com/golang/go/issues/15925
//
// so in the end we use hand-crafted array-like byte-structs.
type be16 struct {
	_0 byte
	_1 byte
}
type be32 struct {
	_0 byte
	_1 byte
	_2 byte
	_3 byte
}
type be64 struct {
	_0 byte
	_1 byte
	_2 byte
	_3 byte
	_4 byte
	_5 byte
	_6 byte
	_7 byte
}

// XXX naming ntoh{s,l,q} ?

// good
func ntoh16(v be16) uint16 {
	b := (*[2]byte)(unsafe.Pointer(&v))
	return binary.BigEndian.Uint16(b[:])
}

// bad (unnecessary MOVBLZX AL, AX + shifts not combined into ROLW $8)
// XXX why?
func _ntoh16_1(v be16) uint16 {
	return  uint16(v._1) | uint16(v._0)<<8
}

// good
func hton16(v uint16) be16 {
	return be16{byte(v>>8), byte(v)}
}

// good
func _hton16_1(v uint16) (r be16) {
	r._0 = byte(v>>8)
	r._1 = byte(v)
	return r
}

// bad (partly (!?) preclears r)
func _hton16_2(v uint16) (r be16) {
	b := (*[2]byte)(unsafe.Pointer(&r))
	binary.BigEndian.PutUint16(b[:], v)
	return r
}

// ----------------------------------------

// good
func ntoh32(v be32) uint32 {
	b := (*[4]byte)(unsafe.Pointer(&v))
	return binary.BigEndian.Uint32(b[:])
}

// baaaadd (unnecessary MOVBLZX AL, AX + shifts not combined into BSWAPL)
// XXX why?
func _ntoh32_1(v be32) uint32 {
	return  uint32(v._3) | uint32(v._2)<<8 | uint32(v._1)<<16 | uint32(v._0)<<24
}

// good
func hton32(v uint32) be32 {
	return be32{byte(v>>24), byte(v>>16), byte(v>>8), byte(v)}
}


// good
func _hton32_1(v uint32) (r be32) {
	r._0 = byte(v>>24)
	r._1 = byte(v>>16)
	r._2 = byte(v>>8)
	r._3 = byte(v)
	return r
}

// bad (partly (!?) preclears r)
func hton32_2(v uint32) (r be32) {
	b := (*[4]byte)(unsafe.Pointer(&r))
	binary.BigEndian.PutUint32(b[:], v)
	return r
}

// ----------------------------------------

// good
func ntoh64(v be64) uint64 {
	b := (*[8]byte)(unsafe.Pointer(&v))
	return binary.BigEndian.Uint64(b[:])
}

// baad (+local temp; r = temp)
func hton64(v uint64) be64 {
	return be64{byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		    byte(v>>24), byte(v>>16), byte(v>>8),  byte(v)}
}

// bad (pre-clears r)
func hton64_1(v uint64) (r be64) {
	r._0 = byte(v>>56)
	r._1 = byte(v>>48)
	r._2 = byte(v>>40)
	r._3 = byte(v>>32)
	r._4 = byte(v>>24)
	r._5 = byte(v>>16)
	r._6 = byte(v>>8)
	r._7 = byte(v)
	return r
}

// bad (pre-clears r)
func hton64_2(v uint64) (r be64) {
	b := (*[8]byte)(unsafe.Pointer(&r))
	binary.BigEndian.PutUint64(b[:], v)
	return r
}

// bad (pre-clears r)
func hton64_3(v uint64) (r be64) {
	b := (*[8]byte)(unsafe.Pointer(&v))
	*(*uint64)(unsafe.Pointer(&r)) = binary.BigEndian.Uint64(b[:])
	return
}


type A struct {
	z be16
}

func zzz(a *A, v uint16) {
	a.z = hton16(v)
}
