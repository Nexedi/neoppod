// TODO copyright / license

// XXX move -> xbufio

package fs1

import (
	"io"

	//"log"
)

// XXX for backward/forward changing workloads (e.g. fstail) there can be cases like:
//	         --------
//	        v        |
//	+-----------+----^-----+
//	|       H   |    T     |
//	+-----------+----------+
//          page2      page1
//
// so jumping back from T(ail) to H(ead) will drop buffer for page1 and load it
// with page2, but next forward read till T will need to again access page1 so
// page2 will be dropped, page1 reloaded, then page2 is needed again (for prev
// tail) and is reloaded again.
//
// This can be avoided with keeping 2 buffers (to always be able to compensate
// direction change) but I'm not sure it will not degrade allways forward case
// becuase pressure to cache is increased 2x.
//
// -> if/when we really need it implement 2 buffers approach.
//
// XXX this also can be solved via loading not from posLastIO but from posLastAccess.


// SeqBufReader implements buffering for a io.ReaderAt optimized for sequential access
// Both forward, backward and interleaved forward/backward access patterns are supported	XXX
// FIXME access from multiple goroutines? (it is required per io.ReaderAt
// interface, but for sequential workloads we do not need it)
// XXX -> xbufio.SeqReader
type SeqBufReader struct {
	// buffer for data at pos. cap(buf) - whole buffer capacity
	buf	[]byte
	pos	int64

	// // position of last IO  (can be != .pos because large reads are not buffered)
	// posLastIO int64

	// TODO text
	posLastAccess	int64
	posLastFwdAfter	int64
	posLastBackward	int64

	r io.ReaderAt
}

// TODO text about syscall / memcpy etc
const defaultSeqBufSize = 8192	// XXX retune - must be <= size(L1d) / 2

func NewSeqBufReader(r io.ReaderAt) *SeqBufReader {
	return NewSeqBufReaderSize(r, defaultSeqBufSize)
}

func NewSeqBufReaderSize(r io.ReaderAt, size int) *SeqBufReader {
	sb := &SeqBufReader{r: r, pos: 0, buf: make([]byte, 0, size)}	//, posLastIO: 0}
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
		//sb.posLastIO = pos
		// TODO update lastAccess & lastFwd/lastBack
		return sb.r.ReadAt(p, pos)
	}

	var nhead int // #data read from buffer for p head
	var ntail int // #data read from buffer for p tail

	// try to satisfy read request via (partly) reading from buffer

	// use buffered data: start + forward
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

	// use buffered data: tail + backward
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

	//if pos >= sb.posLastIO {
	if pos >= posLastAccess {
		// forward
		xpos = pos

		// if forward trend continues and buffering can be made adjacent to
		// previous forward access - shift reading down right to after it.
		xLastAfter := posLastFwdAfter + int64(nhead)	// XXX comment
		if xLastAfter <= xpos && xpos + len64(p) <= xLastAfter + cap64(sb.buf) {
			xpos = xLastAfter
		}

		// XXX symmetry for "alternatively" in backward case

	} else {
		// backward
		xpos = pos

		// if backward trend continues and bufferring would overlap with
		// previous backward access - shift reading up right to it.
		if xpos < posLastBackward && posLastBackward < xpos + cap64(sb.buf) {
			xpos = max64(posLastBackward, xpos + len64(p)) - cap64(sb.buf)

		// XXX recheck do we really need this ?	( was added for {122, 6, 121, 10} )
		// XXX alternatively even if backward trend does not continue anymore
		// but if this will overlap with last access (XXX load) range, probably
		// it is better (we are optimizing for sequential access) to
		// shift loading region down not to overlap.
		} else if xpos + cap64(sb.buf) > posLastAccess {
			xpos = max64(posLastAccess, xpos + len64(p)) - cap64(sb.buf)
		}

		// don't let reading go beyond start of the file
		xpos = max64(xpos, 0)

		/*
		// by default we want to read forward, even when iterating backward:
		// there are frequent jumps backward for reading a record there forward
		xpos = pos

		// but if this will overlap with last access range, probably
		// it is better (we are optimizing for sequential access) to
		// shift loading region down not to overlap.
		//
		// we have to take into account that last and current access regions
		// can overlap, if e.g. last access was big non-buffered read.
		if xpos + cap64(sb.buf) > sb.posLastIO {
			xpos = max64(sb.posLastIO, xpos + len64(p)) - cap64(sb.buf)

			// don't let reading go beyond start of the file
			xpos = max64(xpos, 0)
		}
		*/
	}

	//log.Printf("read [%v, %v)\t#%v", xpos, xpos + cap64(sb.buf), cap(sb.buf))
//	sb.posLastIO = xpos
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
//			sb.posLastIO = pos
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
