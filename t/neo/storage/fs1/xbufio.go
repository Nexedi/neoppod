// TODO copyright / license

// XXX move -> xbufio

package fs1

// SeqBufReader implements buffering for a io.ReaderAt optimized for sequential access
// XXX -> xbufio.SeqReader
type SeqBufReader struct {
	r io.ReaderAt

	// buffer for data at pos. cap(buf) - whole buffer capacity
	pos	int64
	buf	[]byte

	// detected io direction (0 - don't know yet, >0 - forward, <0 - backward)
	// XXX strictly 0, +1, -1	?
	dir	int
}


// readFromBuf reads as much as possible for ReadAt(p, pos) request from buffered data
// it returns nread and (p', pos') that was left for real reading to complete
// XXX dir?
func (sb *SeqBufReader) readFromBuf(p []byte, pos int64) (int, []byte, int64) {
	// use buffered data: start + forward
	if pos >= sb.pos && pos < sb.pos + len(sb.buf) {
		copyPos := pos - sb.pos
		n = copy(p, sb.buf[copyPos:]) // NOTE len(p) can be < len(sb[copyPos:])
		p = p[n:]
		pos += n
		dir = +1
	}

	// XXX use buffered data: tail + backward
	if pos + len(p) <= sb.pos + len(sb.buf) && pos + len(p) > sb.pos {
		// TODO
		dir = -1
	}

	return n, p, pos

}

func (sb *SeqBufReader) ReadAt(p []byte, pos int64) (int, error) {
	// if request size > buffer - read data directly
	if len(p) > cap(sb.buf) {
		dir = sign(pos - sb.pos) // XXX recheck
		return sb.r.ReadAt(p, pos)
	}

	n, p, pos = sb.readFromBuf(p, pos)

	// all was read from buffer
	if len(p) == 0 {
		return n, nil
	}

	// otherwise we need to refill the buffer. determine range to read by current IO direction.
	switch {
	case dir == 0:
		// we don't know direction yet - usually it is first request.
		// cover pos/p symmetrically. This way we will give hopefully
		// give enough overlap for next read request to determine
		// direction.
		xpos = pos - (cap(sb.buf) - len(p)) / 2
		if xpos < 0 {
			xpos = 0
		}

	case dir > 0:
		// forward
		xpos = pos

	default:
		// backward
		//xpos = max64(pos - cap(sb.buf), 0)
		xpos = pos - cap(sb.buf)
		if xpos < 0 {
			xpos = 0
		}
	}

	buf = sb.buf[:cap(sb.buf)]
	nn, err = sb.r.ReadAt(buf, xpos)

	// even if there was an error, e.g. after reading part, we remember data read in buffer
	// XXX only `if nn > 0` ?
	sb.pos = xpos
	sb.buf = buf[:nn]

	// here we know:
	// - some data was read	XXX recheck
	// - len(p) < cap(sb.buf)
	// - there is overlap in between pos/p vs sb.pos/sb.buf	XXX recheck
	// try to read again what is left to read from the buffer
	nn, p, pos = sb.readFromBuf(p, pos)
	n += nn

	// now if there was an error - we can skip it if original read request
	// was completely satisfied
	if len(p) == 0 {
		// XXX preserve EOF at ends? - not needed per ReaderAt interface
		err = nil
	}

	// all done
	return n, err
}
