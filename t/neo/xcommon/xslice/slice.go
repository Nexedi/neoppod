// Package xslice provides utilities for working with slices
package xslice

import (
	"../xmath"
)

// Grow increase length of slice by n elements.
// If there is not enough capacity the slice is reallocated.
// The memory for grown elements are not initialized.
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
