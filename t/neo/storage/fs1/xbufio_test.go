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

	// read @pos/len -> rb.pos, len(rb.buf), rb.dir
	testv := []struct {pos int64; Len int; bufPos int64; bufLen, bufDir int} {
		{40,  5, 38, 10, 0},	// 1st access - symmetrically covered
		{45,  5, 48, 10, +1},	// part taken from buf, part read next, forward detected
		{50,  5, 48, 10, +1},	// everything taken from buf
		{55,  5, 58, 10, +1},	// part taken from buf, part read next
		{60, 10, 58, 10, +1},	// access bigger than buf
		{70, 10, 58, 10, +1},	// access bigger than buf, once again

		// XXX big access going backward - detect dir change

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
		if !(rb.pos == tt.bufPos && len(rb.buf) == tt.bufLen && rb.dir == tt.bufDir) {
			t.Fatalf("%v: -> unexpected buffer state @%v #%v %+d", tt, rb.pos, len(rb.buf), rb.dir)
		}
	}
}
