// Copyright (C) 2017  Nexedi SA and Contributors.
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

// Package xfmt provides addons to std fmt and strconv packages with focus on
// formatting text without allocations.
//
// For example if in fmt speak you have
//
//	s := fmt.Sprintf("hello %q %d %x", "world", 1, []byte("data"))
//
// xfmt analog would be
//
//	xbuf := xfmt.Buffer{}
//	xbuf .S("hello ") .Qs("world") .C(' ') .D(1) .C(' ') .Xb([]byte("data"))
//	s := xbuf.Bytes()
//
// xfmt.Buffer can be reused several times via Buffer.Reset() .
package xfmt

import (
	"encoding/hex"
	"strconv"
	"unicode/utf8"

	"../xslice"

	"lab.nexedi.com/kirr/go123/mem"
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

// Buffer provides syntactic sugar for formatting mimicking fmt.Printf style
// XXX combine with bytes.Buffer ?
type Buffer []byte

// Reset empties the buffer keeping underlying storage for future formattings
func (b *Buffer) Reset() {
	*b = (*b)[:0]
}

// Bytes returns buffer storage as []byte
func (b Buffer) Bytes() []byte {
	return []byte(b)
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

// V, similarly to %v, adds x formatted by default rules
// XXX -> V(interface {}) ?
func (b *Buffer) V(x Stringer) *Buffer {
	*b = Append(*b, x)
	return b
}

// S appends string formatted by %s
func (b *Buffer) S(s string) *Buffer {
	*b = append(*b, s...)
	return b
}

// Sb appends []byte formatted by %s
func (b *Buffer) Sb(x []byte) *Buffer {
	*b = append(*b, x...)
	return b
}

// Cb appends byte formated by %c
func (b *Buffer) Cb(c byte) *Buffer {
	*b = append(*b, c)
	return b
}


// AppendRune appends to b UTF-8 encoding of r
func AppendRune(b []byte, r rune) []byte {
	l := len(b)
	b = xslice.Grow(b, utf8.UTFMax)
	n := utf8.EncodeRune(b[l:], r)
	return b[:l+n]
}

// C appends rune formatted by %c
func (b *Buffer) C(r rune) *Buffer {
	*b = AppendRune(*b, r)
	return b
}

// D appends int formatted by %d
func (b *Buffer) D(i int) *Buffer {
	*b = strconv.AppendInt(*b, int64(i), 10)
	return b
}

// D64 appends int64 formatted by %d
func (b *Buffer) D64(i int64) *Buffer {
	*b = strconv.AppendInt(*b, i, 10)
	return b
}

// X appends int formatted by %x
func (b *Buffer) X(i int) *Buffer {
	*b = strconv.AppendInt(*b, int64(i), 16)
	return b
}

// AppendHex appends to b hex representation of x
func AppendHex(b []byte, x []byte) []byte {
	lx := hex.EncodedLen(len(x))
	lb := len(b)
	b = xslice.Grow(b, lx)
	hex.Encode(b[lb:], x)
	return b
}

// Xb appends []byte formatted by %x
func (b *Buffer) Xb(x []byte) *Buffer {
	*b = AppendHex(*b, x)
	return b
}

// Xs appends string formatted by %x
func (b *Buffer) Xs(x string) *Buffer {
	return b.Xb(mem.Bytes(x))
}

// TODO XX = %X

// AppendHex016 appends to b x formatted 16-character hex string
func AppendHex016(b []byte, x uint64) []byte {
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

// X016, similarly to %016x, adds hex representation of uint64 x
func (b *Buffer) X016(x uint64) *Buffer {
	*b = AppendHex016(*b, x)
	return b
}

// TODO Qs Qb ?
