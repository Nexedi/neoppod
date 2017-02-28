// Copyright (C) 2017  Nexedi SA and Contributors.
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
//
// XXX partly based on code from ZODB ?
// TODO link to format in zodb/py

// FileStorage v1. Index
package fs1

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	"../../zodb"
	"./fsb"

	pickle "github.com/kisielk/og-rek"

	"lab.nexedi.com/kirr/go123/mem"
)

// fsIndex is Oid -> Data record position mapping used to associate Oid with
// Data record in latest transaction which changed it.	XXX text
type fsIndex struct {
	*fsb.Tree
}

func fsIndexNew() *fsIndex {
	return &fsIndex{fsb.TreeNew()}
}


// NOTE Get/Set/... are taken as-is from fsb.Tree

// on-disk index format
// (changed in 2010 in https://github.com/zopefoundation/ZODB/commit/1bb14faf)
//
// topPos     position pointing just past the last committed transaction
// (oid[:6], fsBucket)
// (oid[:6], fsBucket)
// ...
// None
//
//
// fsBucket:
// oid[6:8]oid[6:8]oid[6:8]...pos[2:8]pos[2:8]pos[2:8]...


const (
	oidPrefixMask  zodb.Oid = (1<<64-1) ^ (1<<16 - 1)	// 0xffffffffffff0000
	posInvalidMask uint64   = (1<<64-1) ^ (1<<48 - 1)	// 0xffff000000000000
)

// IndexSaveError is the error type returned by index save routines
type IndexSaveError struct {
	Err error // error that occured during the operation
}

func (e *IndexSaveError) Error() string {
	return "index save: " + e.Err.Error()
}

// Save saves index to a writer
func (fsi *fsIndex) Save(topPos int64, w io.Writer) error {
	var err error

	{
		p := pickle.NewEncoder(w)

		err = p.Encode(topPos)
		if err != nil {
			goto out
		}

		var oidb [8]byte
		var posb [8]byte
		var oidPrefixCur zodb.Oid	// current oid[0:6] with [6:8] = 00
		oidBuf := []byte{}		// current oid[6:8]oid[6:8]...
		posBuf := []byte{}		// current pos[2:8]pos[2:8]...
		var t [2]interface{}		// tuple for (oid, fsBucket.toString())

		e, _ := fsi.SeekFirst()
		if e != nil {
			defer e.Close()

			for  {
				oid, pos, errStop := e.Next()
				oidPrefix := oid & oidPrefixMask

				if oidPrefix != oidPrefixCur || errStop != nil {
					// emit (oid[0:6], oid[6:8]oid[6:8]...pos[2:8]pos[2:8]...)
					binary.BigEndian.PutUint64(oidb[:], uint64(oidPrefixCur))
					t[0] = oidb[0:6]
					t[1] = bytes.Join([][]byte{oidBuf, posBuf}, nil)
					err = p.Encode(t)
					if err != nil {
						goto out
					}

					oidPrefixCur = oidPrefix
					oidBuf = oidBuf[:0]
					posBuf = posBuf[:0]
				}

				if errStop != nil {
					break
				}

				// check pos does not overflow 6 bytes
				if uint64(pos) & posInvalidMask != 0 {
					err = fmt.Errorf("entry position too large: 0x%x", pos)
					goto out
				}

				binary.BigEndian.PutUint64(oidb[:], uint64(oid))
				binary.BigEndian.PutUint64(posb[:], uint64(pos))

				oidBuf = append(oidBuf, oidb[6:8]...)
				posBuf = append(posBuf, posb[2:8]...)
			}
		}

		err = p.Encode(pickle.None{})
	}

out:
	if err == nil {
		return err
	}

	if _, ok := err.(*pickle.TypeError); ok {
		panic(err)	// all our types are expected to be supported by pickle
	}

	// otherwise it is an error returned by writer, which should already
	// have filename & op as context.
	return &IndexSaveError{err}
}

// SaveFile saves index to a file
func (fsi *fsIndex) SaveFile(topPos int64, path string) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return &IndexSaveError{err}
	}

	defer func() {
		err2 := f.Close()
		if err2 != nil && err == nil {
			err = &IndexSaveError{err2}
		}
	}()

	err = fsi.Save(topPos, f)
	return

}

// IndexLoadError is the error type returned by index load routines
type IndexLoadError struct {
	Filename string
	Pos      int64
	Err	 error
}

func (e *IndexLoadError) Error() string {
	s := "index load: "
	if e.Filename != "" {
		s += e.Filename + ": "
	}
	if e.Pos != -1 {
		s += "pickle @" + strconv.FormatInt(e.Pos, 10) + ": "
	}
	s += e.Err.Error()
	return s
}

// LoadIndex loads index from a reader
func LoadIndex(r io.Reader) (topPos int64, fsi *fsIndex, err error) {
	var picklePos int64

	{
		var ok bool
		var xtopPos, xv interface{}

		xr := NewBufReader(r)
		// by passing bufio.Reader directly we make sure it won't create one internally
		p := pickle.NewDecoder(xr.Reader)

		picklePos = xr.InputOffset()
		xtopPos, err = p.Decode()
		if err != nil {
			goto out
		}
		topPos, ok = xtopPos.(int64)
		if !ok {
			err = fmt.Errorf("topPos is %T  (expected int64)", xtopPos)
			goto out
		}

		fsi = fsIndexNew()
		var oidb [8]byte
		var posb [8]byte

	loop:
		for {
			// load/decode next entry
			picklePos = xr.InputOffset()
			xv, err = p.Decode()
			if err != nil {
				goto out
			}

			switch xv.(type) {
			default:
				err = fmt.Errorf("invalid entry: type %T", xv)
				goto out

			case pickle.None:
				break loop

			case []interface{}:
				// so far ok
			}

			// unpack entry tuple -> oidPrefix, fsBucket
			v := xv.([]interface{})
			if len(v) != 2 {
				err = fmt.Errorf("invalid entry: len = %i", len(v))
				goto out
			}

			// decode oidPrefix
			xoidPrefixStr := v[0]
			oidPrefixStr, ok := xoidPrefixStr.(string)
			if !ok {
				err = fmt.Errorf("invalid oidPrefix: type %T", xoidPrefixStr)
				goto out
			}
			if l := len(oidPrefixStr); l != 6 {
				err = fmt.Errorf("invalid oidPrefix: len = %i", l)
				goto out
			}
			copy(oidb[:], oidPrefixStr)
			oidPrefix := zodb.Oid(binary.BigEndian.Uint64(oidb[:]))

			// check fsBucket
			xkvStr := v[1]
			kvStr, ok := xkvStr.(string)
			if !ok {
				err = fmt.Errorf("invalid fsBucket: type %T", xkvStr)
				goto out
			}
			if l := len(kvStr); l % 8 != 0 {
				err = fmt.Errorf("invalid fsBucket: len = %i", l)
				goto out
			}

			// load btree from fsBucket entries
			kvBuf := mem.Bytes(kvStr)

			n := len(kvBuf) / 8
			oidBuf := kvBuf[:n*2]
			posBuf := kvBuf[n*2:]

			for i:=0; i<n; i++ {
				oid := zodb.Oid(binary.BigEndian.Uint16(oidBuf[i*2:]))
				oid |= oidPrefix
				copy(posb[2:], posBuf[i*6:])
				pos := int64(binary.BigEndian.Uint64(posb[:]))

				fsi.Set(oid, pos)
			}
		}
	}

out:
	if err == nil {
		return topPos, fsi, err
	}

	return 0, nil, &IndexLoadError{IOName(r), picklePos, err}
}

// LoadIndexFile loads index from a file
func LoadIndexFile(path string) (topPos int64, fsi *fsIndex, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, nil, &IndexLoadError{path, -1, err}
	}

	defer func() {
		err2 := f.Close()
		if err2 != nil && err == nil {
			err = &IndexLoadError{path, -1, err}
			topPos, fsi = 0, nil
		}
	}()

	return LoadIndex(f)
}


// TODO move vvv to common place

// CountReader is an io.Reader that count total bytes read
type CountReader struct {
	io.Reader
	nread int64
}

func (r *CountReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.nread += int64(n)
	return n, err
}

// InputOffset returns current position in input stream
func (r *CountReader) InputOffset() int64 {
	return r.nread
}

// BufReader is a bufio.Reader + bell & whistles
type BufReader struct {
	*bufio.Reader
	cr *CountReader
}

func NewBufReader(r io.Reader) *BufReader {
	// idempotent(BufReader)
	if r, ok := r.(*BufReader); ok {
		return r
	}

	// idempotent(CountReader)
	cr, ok := r.(*CountReader)
	if !ok {
		cr = &CountReader{r, 0}
	}

	return &BufReader{bufio.NewReader(cr), cr}
}

// InputOffset returns current position in input stream
func (r *BufReader) InputOffset() int64 {
	return r.cr.InputOffset() - int64(r.Reader.Buffered())
}

// IOName returns a "filename" associated with io.Reader, io.Writer, net.Conn, ...
// if name cannot be deterined - "" is returned.
func IOName(f interface {}) string {
	switch f := f.(type) {
	case *os.File:
		// XXX better via interface { Name() string } ?
		//     but Name() is too broad compared to FileName()
		return f.Name()

	case net.Conn:
		// XXX not including LocalAddr is ok?
		return f.RemoteAddr().String()

	case *io.LimitedReader:
		return IOName(f.R)


	case *io.PipeReader:
		return "pipe"
	case *io.PipeWriter:
		return "pipe"

	// XXX SectionReader MultiReader TeeReader
	// XXX bufio.Reader bufio.Writer bufio.Scanner
	default:
		return ""
	}
}
