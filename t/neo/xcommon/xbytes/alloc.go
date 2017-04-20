// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Open Source Initiative approved licenses and Convey
// the resulting work. Corresponding source of such a combination shall include
// the source code for all other software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

// (re)allocation routines for []byte

package xbytes

import (
	"../xmath"
)

// Grow increase length of slice by n elements.
// If there is not enough capacity the slice is reallocated.
// The memory for grown elements is not initialized.
func Grow(b []byte, n int) []byte {
	ln := len(b) + n
	if ln <= cap(b) {
		return b[:ln]
	}

	bb := make([]byte, ln, xmath.CeilPow2(uint64(ln)))
	copy(bb, b)
	return bb
}

// Resize resized the slice to be of length n.
// If slice length is increased and there is not enough capacity the slice is reallocated.
// The memory for grown elements, if any, is not initialized.
func Resize(b []byte, n int) []byte {
	if cap(b) >= n {
		return b[:n]
	}

	bb := make([]byte, n, xmath.CeilPow2(uint64(n)))
	copy(bb, b)
	return bb
}


// Realloc resizes the slice to be of length n not preserving content.
// If slice length is increased and there is not enough capacity the slice is reallocated.
// The memory for all elements becomes uninitialized.
// XXX semantic clash with C realloc(3) ? or it does not matter?
func Realloc(b []byte, n int) []byte {
	return Realloc64(b, int64(n))
}

// Realloc64 is the same as Realloc but for size typed as int64
func Realloc64(b []byte, n int64) []byte {
	if int64(cap(b)) >= n {
		return b[:n]
	}

	return make([]byte, n, xmath.CeilPow2(uint64(n)))
}

// TODO MakeRoom
// TODO Prealloc (make sure cap is enough but length stays unchanged)	(was GrowSlice)

// TODO Resize without copy ?

// // GrowSlice makes sure cap(b) >= n.
// // If not it reallocates/copies the slice appropriately.
// // len of returned slice remains original len(b).
// func GrowSlice(b []byte, n int) []byte {
// 	if cap(b) >= n {
// 		return b
// 	}
// 
// 	bb := make([]byte, len(b), CeilPow2(uint64(n)))
// 	copy(bb, b)
// 	return bb
// }
// 
// // makeRoom makes sure len([len(b):cap(b)]) >= n.
// // If it is not it reallocates the slice appropriately.
// // len of returned slice remains original len(b).
// func MakeRoom(b []byte, n int) []byte {
// 	return GrowSlice(b, len(b) + n)
// }
