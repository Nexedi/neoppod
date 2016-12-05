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

// NEO | Work with bigendian data

package neo

import (
	"encoding/binary"
	"unsafe"
)

type be16 uint16
type be32 uint32
type be64 uint64

func ntoh16(v be16) uint16 {
	b := (*[2]byte)(unsafe.Pointer(&v))
	return binary.BigEndian.Uint16(b[:])
}

/* NOTE ^^^ is as efficient
func ntoh16_2(v be16) uint16 {
	b := (*[2]byte)(unsafe.Pointer(&v))
	return uint16(b[1]) | uint16(b[0])<<8
}
*/

/* FIXME compiler emits instruction to pre-clear r, probably because of &r
func hton16_2(v uint16) (r be16) {
	b := (*[2]byte)(unsafe.Pointer(&r))
	binary.BigEndian.PutUint16(b[:], v)
	return r
}
*/

// NOTE here we are leveraging BigEndian.Uint16**2 = identity
func hton16(v uint16) be16 {
	// FIXME just doing
	//	return be16(ntoh16(be16(v)))
	// emits more prologue/epilogue
	b := (*[2]byte)(unsafe.Pointer(&v))
	return be16( binary.BigEndian.Uint16(b[:]) )
}


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
