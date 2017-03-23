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
		if pos >= 100 && pos <= 104 {
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

func TestSeqBufReader(t *testing.T) {
	r := &XReader{}
	rb := NewSeqBufReaderSize(r, 10) // with 10 it is easier to do/check math for a human

	// read @pos/len -> rb.pos, len(rb.buf)		//, rb.dir
	testv := []struct {pos int64; Len int; bufPos int64; bufLen int} {	//, bufDir int} {
		{40,  5, 40, 10},	// 1st access, forward by default
		{45,  7, 50, 10},	// part taken from buf, part read next, forward detected
		{52,  5, 50, 10},	// everything taken from buf
		{57,  5, 60, 10},	// part taken from buf, part read next
		{60, 11, 60, 10},	// access > cap(buf)
		{71, 11, 60, 10},	// access > cap(buf), once again

		// XXX big access going backward - detect dir change

		// TODO accees around and in error range

		// TODO overlap

	}

	for _, tt := range testv {
		pOk := make([]byte, tt.Len)
		pB  := make([]byte, tt.Len)

		nOk, errOk := r.ReadAt(pOk, tt.pos)
		nB,  errB  := rb.ReadAt(pB, tt.pos)

		// check that reading result is the same
		if !(nB == nOk && errB == errOk && bytes.Equal(pB, pOk)) {
			t.Fatalf("%v: -> %v, %#v, %v  ; want %v, %#v, %v", tt, nB, errB, pB, nOk, errOk, pOk)
		}

		// verify buffer state
		if !(rb.pos == tt.bufPos && len(rb.buf) == tt.bufLen){ // && rb.dir == tt.bufDir) {
			t.Fatalf("%v: -> unexpected buffer state @%v #%v", tt, rb.pos, len(rb.buf))	//, rb.dir)
		}
	}
}
