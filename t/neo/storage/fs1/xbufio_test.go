// TODO copyright / license

// XXX move -> xbufio

package fs1

import (
	"bytes"
	"errors"
	"io"
	"testing"
)


// XReader is an io.ReaderAt that reads first 256 bytes with content_i = i
// bytes in range [100, 104] give EIO on reading
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
	{45,  7, 50, 10},	// part taken from buf, part read next, forward
	{52,  5, 50, 10},	// everything taken from buf
	{57,  5, 60, 10},	// part taken from buf, part read next
	{60, 11, 60, 10},	// access > cap(buf), buf skipped
	{71, 11, 60, 10},	// access > cap(buf), once again
	{82, 10, 82, 10},	// access = cap(buf), should refill buf
	{92,  5, 92,  8},	// next access - should refill buffer, but only up to EIO range
	{97,  4, 100, 0},	// this triggers user-visible EIO, buffer scratched
	{101, 5, 101, 0},	// EIO again
	{105, 5, 105, 10},	// past EIO range - buffer refilled
	{88,  8, 88, 10},	// go back again a bit before EIO range
	{96,  6, 98,  2},	// this uses both buffered data + result of next read which hits EIO
	{110,70, 98,  2},	// very big access forward, buf untouched
	{180,70, 98,  2},	// big access ~ forward

	{172, 5, 170, 10},	// backward: buffer refilled up to posLastIO
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
	{128, 4, 126, 10},	// backward: buf refilled up to posLastIO

	{5, 4, 5, 10},		// forward near file start
	{2, 3, 0, 10},		// backward: buf does not go beyong 0

	{40, 0, 0, 10},		// zero-sized out-of-buffer read do not change buffer

	// backward vs EIO
	{110, 1, 110, 10},	// reset state: forward @110
	{105, 7, 100,  0},	// backward client after EIO: buf scratched but read request satisfied
	{110, 1, 110, 10},	// reset @110
	{103, 5, 100,  0},	// backward overlapping tail EIO: buf scratched, EIO -> user
	{110, 1, 110, 10},	// reset @110
	{101, 2, 100,  0},	// backward inside EIO range: buf scratched, EIO -> user
	{108, 1, 108, 10},	// reset @108
	{100, 4,  98,  2},	// backward = EIO range: buf filled < EIO range, EIO -> user
	{108, 1, 108, 10},	// reset @108
	{ 99, 6,  98,  2},	// backward overlapping whole EIO range: buf filled <= EIO range, EIO -> user
	{108, 1, 108, 10},	// reset @108
	{ 99, 4,  98,  2},	// backward overlapping head EIO: buf filled < EIO range, EIO -> user
	{108, 1, 108, 10},	// reset @108
	{ 98, 1,  98,  2},	// nackward not overlapping EIO: buf filled < EIO range

	{250, 4, 250,  6},	// access near EOF - buffer fill hits EOF, but not returns it to client
	{254, 5, 256,  0},	// access overlapping EOF - EOF returned, buf scratched
	{256, 1, 256,  0},	// access past EOF -> EOF
	{257, 1, 257,  0},	// ----//----
}


func TestSeqBufReader(t *testing.T) {
	r := &XReader{}
	rb := NewSeqBufReaderSize(r, 10) // with 10 it is easier to do/check math for a human

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

func BenchmarkSeqBufReader(b *testing.B) {
	r := &XReader{}
	rb := NewSeqBufReaderSize(r, 10) // same as in TestSeqBufReader
	buf := make([]byte, 128 /* > all .Len in xSeqBufTestv */)

	for i := 0; i < b.N; i++ {
		for _, tt := range xSeqBufTestv {
			rb.ReadAt(buf[:tt.Len], tt.pos)
		}
	}
}
