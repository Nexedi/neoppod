// TODO copyright/license

// Package xfmt provide addons to std fmt and strconv packages with focus on
// formatting text without allocations.
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
// XXX -> v(interface {}) ?
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


// AppendRune appends to be UTF-8 encoding of r
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
