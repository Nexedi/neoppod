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

// Grow increases length of byte slice by n elements.
// If there is not enough capacity the slice is reallocated and copied.
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

// MakeRoom makes sure cap(b) - len(b) >= n
// If there is not enough capacity the slice is reallocated and copied.
// Length of the slice remains unchanged.
func MakeRoom(b []byte, n int) []byte {
	ln := len(b) + n
	if ln <= cap(b) {
		return b
	}

	bb := make([]byte, len(b), xmath.CeilPow2(uint64(ln)))
	copy(bb, b)
	return bb
}

// Resize resized byte slice to be of length n.
// If slice length is increased and there is not enough capacity the slice is reallocated and copied.
// The memory for grown elements, if any, is not initialized.
func Resize(b []byte, n int) []byte {
	if cap(b) >= n {
		return b[:n]
	}

	bb := make([]byte, n, xmath.CeilPow2(uint64(n)))
	copy(bb, b)
	return bb
}


// Realloc resizes byte slice to be of length n not preserving content.
// If slice length is increased and there is not enough capacity the slice is reallocated but not copied.
// The memory for all elements becomes uninitialized.
//
// NOTE semantic is different from C realloc(3) where content is preserved
// NOTE use Resize when you need to preserve slice content
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
