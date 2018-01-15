// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

package fs1

import (
	"io"
	"os"

	"lab.nexedi.com/kirr/go123/xbufio"
)

// noEOF returns err, but changes io.EOF -> io.ErrUnexpectedEOF
func noEOF(err error) error {
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return err
}

// okEOF returns err, but changes io.EOF -> nil
func okEOF(err error) error {
	if err == io.EOF {
		err = nil
	}
	return err
}

// record reading routines work on abstract file-like interfaces.
// when they report e.g. decoding error, if reader has name, e.g. as os.File
// does, this name is included into error as prefix.

// named represents something having a name - ex. os.File
type named interface {
	Name() string
}

// ioname returns f's name if it has one.
// if not - "" is returned.
func ioname(f interface{}) string {
	if fn, ok := f.(named); ok {
		return fn.Name()
	}
	return ""
}

// ioprefix returns "<name>: " for f if it has name.
// if not - "" is returned.
func ioprefix(f interface{}) string {
	n := ioname(f)
	if n != "" {
		return n + ": "
	}
	return ""
}


// seqReadAt wraps os.File with xbufio.SeqReaderAt for its .ReadAt method.
// in particular f's name is preserved.
func seqReadAt(f *os.File) *seqFileReaderAt {
	fseq := xbufio.NewSeqReaderAt(f)
	return &seqFileReaderAt{fseq, f}
}

type seqFileReaderAt struct {
	*xbufio.SeqReaderAt

	// keep os.File still exposed for .Name()
	// XXX better xbufio.SeqReaderAt expose underlying reader?
	*os.File
}

func (f *seqFileReaderAt) ReadAt(p []byte, off int64) (int, error) {
	// route ReadAt to wrapper
	return f.SeqReaderAt.ReadAt(p, off)
}
