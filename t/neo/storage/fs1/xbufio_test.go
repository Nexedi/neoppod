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
	{97,  4, 92,  8},	// this triggers user-visible EIO, buffer not refilled
	{101, 5, 92,  8},	// EIO again
	{105, 5, 105, 10},	// past EIO range - buffer refilled
	{110,70, 105, 10},	// very big access forward, buf untouched
	{180,70, 105, 10},	// big access ~ forward
	{170,11, 105, 10},	// big access backward
	{160,11, 105, 10},	// big access backward, once more
	{155, 5, 155, 10},	// access backward - buffer refilled
				// XXX refilled forward first time after big backward readings
	{150, 5, 145, 10},	// next access backward - buffer refilled backward
	{143, 7, 135, 10},	// backward once again - buffer refilled backward

	{250, 4, 250,  6},	// access near EOF - buffer fill hits EOF, but not returns it to client
	{254, 5, 250,  6},	// access overlapping EOF - EOF returned
	{256, 1, 250,  6},	// access past EOF -> EOF
	{257, 1, 250,  6},	// ----//----
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
