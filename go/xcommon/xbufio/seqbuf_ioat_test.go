// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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

package xbufio

import (
	"bytes"
	"errors"
	"io"
	"testing"
)


// XReader is an io.ReaderAt that reads first 256 bytes with content_i = i.
// bytes in range [100, 104] give EIO on reading.
type XReader struct {
}

var EIO = errors.New("input/output error")

func (r *XReader) ReadAt(p []byte, pos int64) (n int, err error) {
	for n < len(p) && pos < 0x100 {
		if 100 <= pos && pos <= 104 {
			err = EIO
			break
		}

		p[n] = byte(pos)
		n++
		pos++
	}

	if n < len(p) && err == nil {
		err = io.EOF
	}

	return n, err
}


// read @pos/len -> rb.pos, len(rb.buf)
var xSeqBufTestv = []struct {pos int64; Len int; bufPos int64; bufLen int} {
	{40,  5, 40, 10},	// 1st access, forward by default
	{45,  7, 50, 10},	// part taken from buf, part read next, forward (trend)
	{52,  5, 50, 10},	// everything taken from buf
	{57,  5, 60, 10},	// part taken from buf, part read next (trend)
	{60, 11, 60, 10},	// access > cap(buf), buf skipped
	{71, 11, 60, 10},	// access > cap(buf), once again
	{82, 10, 82, 10},	// access = cap(buf), should refill buf
	{92,  5, 92,  8},	// next access - should refill buffer (trend), but only up to EIO range
	{97,  4, 100, 0},	// this triggers user-visible EIO, buffer scratched
	{101, 5, 101, 0},	// EIO again
	{105, 5, 105, 10},	// past EIO range - buffer refilled
	{88,  8, 88, 10},	// go back again a bit before EIO range
	{96,  6, 98,  2},	// this uses both buffered data + result of next read which hits EIO
	{110,70, 98,  2},	// very big access forward, buf untouched
	{180,70, 98,  2},	// big access ~ forward

	{170, 5, 170, 10},	// backward: buffer refilled forward because prev backward reading was below
	{168, 4, 160, 10},	// backward: buffer refilled backward
	{162, 6, 160, 10},	// backward: all data read from buffer
	{150,12, 160, 10},	// big backward: buf untouched
	{142, 6, 140, 10},	// backward: buffer refilled up to posLastIO
	{130,12, 140, 10},	// big backward: buf untouched
	{122, 9, 121, 10},	// backward overlapping with last bigio: buf correctly refilled
	{131, 9, 131, 10},	// forward after backward: buf refilled forward
	{122, 6, 121, 10},	// backward after forward: buf refilled backward
	{131, 9, 131, 10},	// forward again
	{136,20, 131, 10},	// big forward starting from inside filled buf
	{128, 4, 126, 10},	// backward (not trend): buf refilled up to posLastIO

	// interleaved backward + fwd-fwd-fwd
	{200,10, 200, 10},	// reset @200
	{194, 1, 190, 10},	// 1st backward access: buf refilled
	{186, 1, 184, 10},	// trendy backward access - buf refilled up-to prev back read

	{187, 1, 184, 10},	// fwd-fwd-fwd (all already buffered)
	{188, 2, 184, 10},
	{190, 3, 184, 10},

	{182, 4, 174, 10},	// trendy backward access - part taken from buffer and buf refilled adjacent to previous backward IO

	{168, 1, 168, 10},	// trendy backward access farther than cap(buf) - buf refilled right at @pos

	{169, 7, 168, 10},	// fwd-fwd-fwd (partly buffered / partly loaded)
	{176, 3, 178, 10},
	{179, 6, 178, 10},

	// interleaved forward + back-back-back
	{200,10, 200, 10},	// reset @200
	{206, 1, 200, 10},	// 1st forward access
	{214, 1, 207, 10},	// trendy forward access - buf refilled adjacent to previous forward read

	{213, 1, 207, 10},	// back-back-back (all already buffered)
	{211, 2, 207, 10},
	{207, 5, 207, 10},

	{215, 4, 217, 10},	// trendy forward access - part taken from buffer and buf refilled adjacent to previous forward IO

	{235, 1, 235, 10},	// trendy forward access farther than cap(buf) - buf refilled right at @pos

	{234, 1, 225, 10},	// back-back-back (partly loaded / then partly buffered)
	{230, 3, 225, 10},
	{222, 8, 215, 10},
	{219, 3, 215, 10},

	// backward (non trend) vs not overlapping previous forward
	{230,10, 230, 10},	// forward  @230 (1)
	{220,10, 220, 10},	// backward @220 (2)
	{250, 4, 250,  6},	// forward  @250 (3)
	{245, 5, 240, 10},	// backward @245 (4)

	{5, 4, 5, 10},		// forward near file start
	{2, 3, 0, 10},		// backward: buf does not go beyond 0

	{40, 0, 0, 10},		// zero-sized out-of-buffer read do not change buffer

	// backward (not trend) vs EIO
	{108,10, 108, 10},	// reset @108
	{ 98, 1,  98,  2},	// backward not overlapping EIO: buf filled < EIO range
	{108,10, 108, 10},	// reset @108
	{ 99, 4,  98,  2},	// backward overlapping head EIO: buf filled < EIO range, EIO -> user
	{108,10, 108, 10},	// reset @108
	{ 99, 6,  98,  2},	// backward overlapping whole EIO range: buf filled <= EIO range, EIO -> user
	{108,10, 108, 10},	// reset @108
	{100, 4,  98,  2},	// backward = EIO range: buf filled < EIO range, EIO -> user
	{110,10, 110, 10},	// reset @110
	{101, 2, 100,  0},	// backward inside EIO range: buf scratched, EIO -> user
	{110,10, 110, 10},	// reset @110
	{103, 5, 100,  0},	// backward overlapping tail EIO: buf scratched, EIO -> user
	{110,10, 110, 10},	// reset state: forward @110
	{105, 7, 100,  0},	// backward client after EIO: buf scratched but read request satisfied

	// backward (trend) vs EIO
	// NOTE this is reverse of `backward (not trend) vs EIO
	{110,10, 110, 10},	// reset state: forward @110
	{105, 7, 100,  0},	// backward client after EIO: buf scratched but read request satisfied
	{110,10, 110, 10},	// reset @110
	{103, 5,  98,  2},	// backward overlapping tail EIO: buf scratched (XXX), EIO -> user
	{110,10, 110, 10},	// reset @110
	{101, 2,  93,  7},	// backward inside EIO range: buf scratched (XXX), EIO -> user
	{108,10, 108, 10},	// reset @108
	{100, 4,  94,  6},	// backward = EIO range: buf filled < EIO range, EIO -> user
	{108,10, 108, 10},	// reset @108
	{ 99, 6,  95,  5},	// backward overlapping whole EIO range: buf filled <= EIO range, EIO -> user
	{108,10, 108, 10},	// reset @108
	{ 99, 4,  98,  2},	// backward overlapping head EIO: buf filled < EIO range, EIO -> user
	{108,10, 108, 10},	// reset @108
	{ 98, 1,  89, 10},	// backward not overlapping EIO: buf filled according to backward trend

	// forward (trend) vs EIO
	{  0, 1,   0, 10},
	{ 88,10,  88, 10},	// reset forward @98
	{ 98, 1,  98,  2},	// forward not overlapping EIO: buf filled < EIO range
	{  0, 1,   0, 10},
	{ 88,10,  88, 10},	// reset forward @98
	{ 99, 4,  98,  2},	// forward overlapping head EIO: buf filled < EIO range, EIO -> user
	{  0, 1,   0, 10},
	{ 88,10,  88, 10},	// reset forward @98
	{ 99, 6,  98,  2},	// forward overlapping whole EIO range: buf filled <= EIO range, EIO -> user
	{  0, 1,   0, 10},
	{ 88,10,  88, 10},	// reset forward @98
	{100, 4,  98,  2},	// forward = EIO range: buf filled < EIO range, EIO -> user
	{  0, 1,   0, 10},
	{ 90,10,  90, 10},	// reset forward @100
	{101, 2, 100,  0},	// forward inside EIO range: buf scratched, EIO -> user
	{  0, 1,   0, 10},
	{ 90,10,  90, 10},	// reset forward @100
	{103, 5, 100,  0},	// forward overlapping tail EIO: buf scratched, EIO -> user
	{  0, 1,   0, 10},
	{ 90,10,  90, 10},	// reset forward @100
	{105, 2, 100,  0},	// forward client after EIO: buf scratched but read request satisfied

	{  0, 1,   0, 10},
	{ 90, 5,  90, 10},	// reset forward @95
	{ 99, 3,  96,  4},	// forward jump client overlapping head EIO: buf filled < EIO range, EIO -> user

	{  0, 1,   0, 10},
	{ 89, 5,  89, 10},	// reset forward @94
	{ 98, 2,  95,  5},	// forward jump client reading < EIO: buf filled < EIO range, user request satisfied


	// EOF handling
	{250, 4, 250,  6},	// access near EOF - buffer fill hits EOF, but not returns it to client
	{254, 5, 256,  0},	// access overlapping EOF - EOF returned, buf scratched
	{256, 1, 256,  0},	// access past EOF -> EOF
	{257, 1, 257,  0},	// ----//----

	// forward with jumps - buffer is still refilled adjacent to previous reading
	// ( because jumps are not sequential access and we are optimizing for sequential cases.
	//   also: if jump > cap(buf) reading will not be adjacent)
	{ 0, 1,  0, 10},	// reset
	{ 0, 5,  0, 10},
	{ 9, 3,  6, 10},
	{20, 3, 20, 10},
}


func TestSeqReaderAt(t *testing.T) {
	r := &XReader{}
	rb := NewSeqReaderAtSize(r, 10) // with 10 it is easier to do/check math for a human

	for _, tt := range xSeqBufTestv {
		pOk := make([]byte, tt.Len)
		pB  := make([]byte, tt.Len)

		nOk, errOk := r.ReadAt(pOk, tt.pos)
		nB,  errB  := rb.ReadAt(pB, tt.pos)

		pOk = pOk[:nOk]
		pB  = pB[:nB]

		// check that reading result is the same
		if !(bytes.Equal(pB, pOk) && errB == errOk) {
			t.Fatalf("%v: -> %v, %#v  ; want %v, %#v", tt, pB, errB, pOk, errOk)
		}

		// verify buffer state
		if !(rb.pos == tt.bufPos && len(rb.buf) == tt.bufLen){
			t.Fatalf("%v: -> unexpected buffer state @%v #%v", tt, rb.pos, len(rb.buf))
		}
	}
}

// this is benchmark for how thin wrapper is, not for logic inside it
func BenchmarkSeqReaderAt(b *testing.B) {
	r := &XReader{}
	rb := NewSeqReaderAtSize(r, 10) // same as in TestSeqReaderAt
	buf := make([]byte, 128 /* > all .Len in xSeqBufTestv */)

	for i := 0; i < b.N; i++ {
		for _, tt := range xSeqBufTestv {
			rb.ReadAt(buf[:tt.Len], tt.pos)
		}
	}
}
