// TODO copyright/license

// Package xfmt provide addons to std fmt and strconv package with focus on
// formatting text without allocations.
package xfmt

import (
	"encoding/hex"

	"../xslice"
)

const (
	hexdigits = "0123456789abcdef"
)

// Stringer is interface for natively formatting a value representation via xfmt
type Stringer interface {
	// XFmtString method is used to append formatted value to destination buffer
	// The grown buffer have to be returned
	XFmtString(b []byte) []byte
}

// Append appends to b formatted x
//
// NOTE sadly since x is interface it makes real value substituted to it
// 	escape to heap (not so a problem since usually they already are) but then also
// 	if x has non-pointer receiver convT2I _allocates_ memory for the value copy.
//
//	-> always pass to append &object, even if object has non-pointer XFmtString receiver.
func Append(b []byte, x Stringer) []byte {
	return x.XFmtString(b)
}

// AppendHex appends to b hex representation of x
func AppendHex(b []byte, x []byte) []byte {
	lx := hex.EncodedLen(len(x))
	lb := len(b)
	b = xslice.Grow(b, lx)
	hex.Encode(b[lb:], x)
	return b
}

// AppendHex64 appends to b x formateed 16-character hex string
func AppendHex64(b []byte, x uint64) []byte {
        // like sprintf("%016x") but faster and less allocations
	l := len(b)
        b = xslice.Grow(b, 16)
	bb := b[l:]
        for i := 15; i >= 0; i-- {
                bb[i] = hexdigits[x & 0xf]
                x >>= 4
        }
	return b
}
