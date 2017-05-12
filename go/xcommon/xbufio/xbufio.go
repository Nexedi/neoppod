// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Open Source Initiative approved licenses and Convey
// the resulting work. Corresponding source of such a combination shall include
// the source code for all other software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

// Package xbufio provides addons to std bufio package.
package xbufio

import (
	"bufio"
	"io"

	"../xio"
)

// Reader is a bufio.Reader + bell & whistles
type Reader struct {
	*bufio.Reader
	cr *xio.CountReader
}

func NewReader(r io.Reader) *Reader {
	// idempotent(Reader)
	if r, ok := r.(*Reader); ok {
		return r
	}

	// idempotent(xio.CountReader)
	cr, ok := r.(*xio.CountReader)
	if !ok {
		cr = &xio.CountReader{r, 0}
	}

	return &Reader{bufio.NewReader(cr), cr}
}

// InputOffset returns current position in input stream
// XXX naming + define interface for getting current position
func (r *Reader) InputOffset() int64 {
	return r.cr.Nread - int64(r.Reader.Buffered())
}
