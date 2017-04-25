// Copyright (C) 2017  Nexedi SA and Contributors.	XXX -> GPLv3
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 2, or (at your
// option) any later version, as published by the Free Software Foundation.
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
