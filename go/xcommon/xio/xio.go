// TODO copyright / license

// Package xio provides addons to standard package io.
package xio

import (
	"fmt"
	"io"
	"net"
	"os"
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

// LimitedWriter is like io.LimitedReader but for writes
type LimitedWriter struct {
        io.Writer
        N int64
}

func (l *LimitedWriter) Write(p []byte) (n int, err error) {
        if l.N <= 0 {
                return 0, io.EOF
        }
        if int64(len(p)) > l.N {
                p = p[0:l.N]
        }
        n, err = l.Writer.Write(p)
        l.N -= int64(n)
        return
}

func LimitWriter(w io.Writer, n int64) io.Writer { return &LimitedWriter{w, n} }


// Name returns a "filename" associated with io.Reader, io.Writer, net.Conn, ...
func Name(f interface {}) string {
	switch f := f.(type) {
	case *os.File:
		// XXX better via interface { Name() string } ?
		//     but Name() is too broad compared to FileName()
		return f.Name()

	case net.Conn:
		// XXX not including LocalAddr is ok?
		return f.RemoteAddr().String()

	case *CountReader:	return Name(f.Reader)
	case *io.LimitedReader:	return Name(f.R)
	case *LimitedWriter:	return Name(f.Writer)
	case *io.PipeReader:	return "pipe"
	case *io.PipeWriter:	return "pipe"

	// XXX SectionReader MultiReader TeeReader
	// XXX bufio.Reader bufio.Writer bufio.Scanner

	// if name cannot be determined - let's provide full info
	default:
		return fmt.Sprintf("%#v", f)
	}
}
