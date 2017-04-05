// TODO copyright / license

// XXX move -> xbufio

package fs1

import (
	"io"

	//"log"
)

// SeqBufReader implements buffering for a io.ReaderAt optimized for sequential access
// Both forward, backward and interleaved forward/backward access patterns are supported
//
// XXX access from multiple goroutines? (it is required per io.ReaderAt
// interface, but for sequential workloads we do not need it)
// XXX -> xbufio.SeqReader
type SeqBufReader struct {
	// buffer for data at pos. cap(buf) - whole buffer capacity
	buf	[]byte
	pos	int64

	posLastAccess	int64 // position of last access request
	posLastFwdAfter	int64 // position of last forward access request
	posLastBackward	int64 // position of last backward access request

	r io.ReaderAt
}

// TODO text about syscall / memcpy etc
const defaultSeqBufSize = 8192	// XXX retune - must be <= size(L1d) / 2

func NewSeqBufReader(r io.ReaderAt) *SeqBufReader {
	return NewSeqBufReaderSize(r, defaultSeqBufSize)
}

func NewSeqBufReaderSize(r io.ReaderAt, size int) *SeqBufReader {
	sb := &SeqBufReader{r: r, buf: make([]byte, 0, size)} // all positions are zero initially
	return sb
}


// // XXX temp
// func init() {
// 	log.SetFlags(0)
// }

func (sb *SeqBufReader) ReadAt(p []byte, pos int64) (int, error) {
	// read-in last access positions and update them in *sb with current ones for next read
	posLastAccess := sb.posLastAccess
	posLastFwdAfter := sb.posLastFwdAfter
	posLastBackward := sb.posLastBackward
	sb.posLastAccess = pos
	if pos >= posLastAccess {
		sb.posLastFwdAfter = pos + len64(p)
	} else {
		sb.posLastBackward = pos
	}

	// if request size > buffer - read data directly
	if len(p) > cap(sb.buf) {
		// no copying from sb.buf here at all as if e.g. we could copy from sb.buf, the
		// kernel can copy the same data from pagecache as well, and it will take the same time
		// because for data in sb.buf corresponding page in pagecache has high p. to be hot.
		//log.Printf("READ [%v, %v)\t#%v", pos, pos + len64(p), len(p))
		return sb.r.ReadAt(p, pos)
	}

	var nhead int // #data read from buffer for p head
	var ntail int // #data read from buffer for p tail

	// try to satisfy read request via (partly) reading from buffer

	// use buffered data: head(p)
	if sb.pos <= pos && pos < sb.pos + len64(sb.buf) {
		nhead = copy(p, sb.buf[pos - sb.pos:]) // NOTE len(p) can be < len(sb[copyPos:])

		// if all was read from buffer - we are done
		if nhead == len(p) {
			return nhead, nil
		}

		p = p[nhead:]
		pos += int64(nhead)

	// empty request (possibly not hitting buffer - do not let it go to real IO path)
	// `len(p) != 0` is also needed for backward reading from buffer, so this condition goes before
	} else if len(p) == 0 {
		return 0, nil

	// use buffered data: tail(p)
	} else if posAfter := pos + len64(p);
		sb.pos < posAfter && posAfter <= sb.pos + len64(sb.buf) {
		// here we know pos < sb.pos
		//
		// proof: consider if pos >= sb.pos.
		// Then from `pos <= sb.pos + len(sb.buf) - len(p)` above it follow that:
		//   `pos < sb.pos + len(sb.buf)`  (NOTE strictly < because len(p) > 0)
		// and we come to condition which is used in `start + forward` if
		ntail = copy(p[sb.pos - pos:], sb.buf) // NOTE ntail == len(p[sb.pos - pos:])

		// NOTE no return here: full p read is impossible for backward
		// p filling: it would mean `pos = sb.pos` which in turn means
		// the condition for forward buf reading must have been triggered.

		p = p[:sb.pos - pos]
		// pos stays the same
	}


	// here we need to refill the buffer. determine range to read by current IO direction.
	// NOTE len(p) <= cap(sb.buf)
	var xpos int64 // position for new IO request

	if pos >= posLastAccess {
		// forward
		xpos = pos

		// if forward trend continues and buffering can be made adjacent to
		// previous forward access - shift reading down right to after it.
		xLastFwdAfter := posLastFwdAfter + int64(nhead)	// adjusted for already read from buffer
		if xLastFwdAfter <= xpos && xpos + len64(p) <= xLastFwdAfter + cap64(sb.buf) {
			xpos = xLastFwdAfter
		}

		// NOTE no symmetry handling for "alternatively" in backward case
		// because symmetry would be:
		//
		//	← →   ← →
		//	3 4   2 1
		//
		// but buffer for 4 already does not overlap 3 as for
		// non-trendy forward reads buffer always grows forward.

	} else {
		// backward
		xpos = pos

		// if backward trend continues and bufferring would overlap with
		// previous backward access - shift reading up right to it.
		xLastBackward := posLastBackward - int64(ntail)	// adjusted for already read from buffer
		if xpos < xLastBackward && xLastBackward < xpos + cap64(sb.buf) {
			xpos = max64(xLastBackward, xpos + len64(p)) - cap64(sb.buf)

		// alternatively even if backward trend does not continue anymore
		// but if this will overlap with last access (XXX load) range, probably
		// it is better (we are optimizing for sequential access) to
		// shift loading region down not to overlap. example:
		//
		//	← →   ← →
		//	2 1   4 3
		//
		// here we do not want 4'th buffer to overlap with 3
		} else if xpos + cap64(sb.buf) > posLastAccess {
			xpos = max64(posLastAccess, xpos + len64(p)) - cap64(sb.buf)
		}

		// don't let reading go beyond start of the file
		xpos = max64(xpos, 0)
	}

	//log.Printf("read [%v, %v)\t#%v", xpos, xpos + cap64(sb.buf), cap(sb.buf))
	nn, err := sb.r.ReadAt(sb.buf[:cap(sb.buf)], xpos)

	// even if there was an error, or data partly read, we cannot retain
	// the old buf content as io.ReaderAt can use whole buf as scratch space
	sb.pos = xpos
	sb.buf = sb.buf[:nn]

	// here we know:
	// - some data was read
	// - in case of successful read pos/p lies completely inside sb.pos/sb.buf

	// copy loaded data from buffer to p
	pBufOffset := pos - xpos // offset corresponding to p in sb.buf
	if pBufOffset >= len64(sb.buf) {
		// this can be only due to some IO error

		// if we know:
		// - it was backward reading, and
		// - original requst was narrower than buffer
		// try to satisfy it once again directly
		if pos != xpos {	// FIXME pos != xpos no longer means backward
			//log.Printf("read [%v, %v)\t#%v", pos, pos + len64(p), len(p))
			nn, err = sb.r.ReadAt(p, pos)
			if nn < len(p) {
				return nn, err
			}
			return nn + ntail, nil	// request fully satisfied - we can ignore error
		}

		// Just return the error
		return nhead, err
	}
	nn = copy(p, sb.buf[pBufOffset:])
	if nn < len(p) {
		// some error - do not account tail - we did not get to it
		return nhead + nn, err
	}

	// all ok
	// NOTE if there was an error - we can skip it if original read request was completely satisfied
	// NOTE not preserving EOF at ends - not required per ReaderAt interface
	return nhead + nn + ntail, nil
}


// utilities:

// len and cap as int64 (we frequently need them and int64 is covering int so
// the conversion is not lossy)
func len64(b []byte) int64 { return int64(len(b)) }
func cap64(b []byte) int64 { return int64(cap(b)) }

// min/max
func min64(a, b int64) int64 { if a < b { return a } else { return b} }
func max64(a, b int64) int64 { if a > b { return a } else { return b} }
