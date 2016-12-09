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

// XXX temp
type xbe16 uint16
type xbe32 uint32
type xbe64 uint64

// uintX has alignment requirement =X; [X]byte has alignment requirement 1.
// That's why we can use [X]byte and this way keep a struct packed, even if Go
// does not support packed structs in general.
type be16 [2]byte
type be32 [4]byte
type be64 [8]byte

// XXX this way compiler does not preclears return
// (on [2]byte it preclears)
// https://github.com/golang/go/issues/15925
type zbe16 struct { b0, b1 byte }

// XXX naming ntoh{s,l,q} ?

func ntoh16(v be16) uint16 {
	//b := (*[2]byte)(unsafe.Pointer(&v))
	return binary.BigEndian.Uint16(v[:])
}

func ntoh16_z(v zbe16) uint16 {
	b := (*[2]byte)(unsafe.Pointer(&v))
	return binary.BigEndian.Uint16(b[:])
}

func hton16_z(v uint16) zbe16 {
	return zbe16{byte(v >> 8), byte(v)}
}


/* NOTE ^^^ is as efficient
func ntoh16_2(v be16) uint16 {
	//b := (*[2]byte)(unsafe.Pointer(&v))
	return uint16(v[1]) | uint16(v[0])<<8
}
*/

func hton16_1(v uint16) (r be16) {
	r[0] = byte(v >> 8)
	r[1] = byte(v)
	return r
	//return [2]byte{byte(v >> 8), byte(v)}
}

/* FIXME compiler emits instruction to pre-clear r, probably because of &r */
func hton16_2(v uint16) (r be16) {
	//b := (*[2]byte)(unsafe.Pointer(&r))
	binary.BigEndian.PutUint16(r[:], v)
	return r
}

func hton16_3x(v uint16) xbe16 {
	return *(*xbe16)(unsafe.Pointer(&v))
	//b := (*be16)(unsafe.Pointer(&v))
	//return *b
}

func hton16_3(v uint16) be16 {
	return be16{4,5}
	//return *(*be16)(unsafe.Pointer(&v))
	//b := (*be16)(unsafe.Pointer(&v))
	//return *b
}

// NOTE here we are leveraging BigEndian.Uint16^2 = identity
func hton16(v uint16) xbe16 {
	// FIXME just doing
	//	return be16(ntoh16(be16(v)))
	// emits more prologue/epilogue
	b := (*[2]byte)(unsafe.Pointer(&v))
	return xbe16( binary.BigEndian.Uint16(b[:]) )
}


/*
func ntoh32(v be32) uint32 {
	b := (*[4]byte)(unsafe.Pointer(&v))
	return binary.BigEndian.Uint32(b[:])
}

func hton32(v uint32) be32 {
	b := (*[4]byte)(unsafe.Pointer(&v))
	return be32( binary.BigEndian.Uint32(b[:]) )
}

func ntoh64(v be64) uint64 {
	b := (*[8]byte)(unsafe.Pointer(&v))
	return binary.BigEndian.Uint64(b[:])
}

func hton64(v uint64) be64 {
	b := (*[8]byte)(unsafe.Pointer(&v))
	return be64( binary.BigEndian.Uint64(b[:]) )
}
*/

type A struct {
	z be16
}

func zzz(a *A, v uint16) {
	a.z = hton16_1(v)
}
