// TODO copyright / license

// Package xio provides addons to standard package io.
package xio

import (
	"context"
	"io"

	"lab.nexedi.com/kirr/neo/go/xcommon/log"
	"lab.nexedi.com/kirr/neo/go/xcommon/xcontext"
)

// XXX interface for a Reader/Writer which can report position
// -> Nread(), Nwrote() ?

// CountReader is an io.Reader that count total bytes read
type CountReader struct {
	io.Reader
	Nread int64
}

func (r *CountReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.Nread += int64(n)
	return n, err
}

// TODO func to get position (requiring io.Seeker to just Seek(0, SeekCurrent) is too much)
// XXX  previously used InputOffset(), but generally e.g. for io.Writer "input" is not appropriate


// CloseWhenDone arranges for c to be closed either when ctx is cancelled or
// surrounding function returns.
//
// To work as intended it should be called under defer like this:
//
//	func myfunc(ctx, ...) {
//		defer xio.CloseWhenDone(ctx, c)()
//
// The error - if c.Close() returns with any - is logged.
func CloseWhenDone(ctx context.Context, c io.Closer) func() {
	return xcontext.WhenDone(ctx, func() {
		err := c.Close()
		if err != nil {
			log.Error(ctx, err)
		}
	})
}
