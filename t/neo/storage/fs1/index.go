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
// XXX based on code from ZODB ?

// FileStorage. Index
package fs1

import (
	"bytes"
	"encoding/binary"
	"io"

	"../../zodb"
	"./fsb"

	"lab.nexedi.com/kirr/go123/mem"

	//"github.com/hydrogen18/stalecucumber"
	pickle "github.com/kisielk/og-rek"
)

// fsIndex is Oid -> Tid's position mapping used to associate Oid with latest
// transaction which changed it.
type fsIndex struct {
	fsb.Tree
}


// TODO Encode		(__getstate__)	?
// TODO Decode		(__setstate__)	?

// NOTE Get/Set/... are taken as-is from fsb.Tree

// on-disk index format
// (changed in 2010 in https://github.com/zopefoundation/ZODB/commit/1bb14faf)
//
// topPos     position pointing just past the last committed transaction
// (oid[:6], v.toString)   # ._data[oid]-> v (fsBucket().toString())
// (oid[:6], v.toString)
// ...
// None
//
//
// fsBucket.toString():
// oid[6:8]oid[6:8]oid[6:8]...pos[0:6]pos[0:6]pos[0:6]...


const oidPrefixMask zodb.Oid = ((1<<64-1) ^ (1<<16 -1))	// 0xffffffffffff0000

// IndexIOError is the error type returned by index save and load routines
type IndexIOError struct {
	Op  string	// operation performed - "save" or "load"
	Err error	// error that occured during the operation
}

func (e *IndexIOError) Error() string {
	s := "index " + e.Op + ": " + e.Err.Error()
	return s
}

// Save saves the index to a writer
func (fsi *fsIndex) Save(topPos int64, w io.Writer) error {
	p := pickle.NewEncoder(w)

	err := p.Encode(topPos)
	if err != nil {
		goto out
	}

	var oidb [8]byte
	var posb [8]byte
	var oidPrefixCur zodb.Oid	// current oid[0:6] with [6:8] = 00
	oidBuf := []byte{}		// current oid[6:8]oid[6:8]...
	posBuf := []byte{}		// current pos[0:6]pos[0:6]...
	var t [2]interface{}		// tuple for (oid, fsBucket.toString())


	e, err := fsi.SeekFirst()
	if err == io.EOF {	// always only io.EOF indicating an empty btree
		goto skip
	}

	for {
		oid, pos, errStop := e.Next()
		oidPrefix := oid & oidPrefixMask

		if oidPrefix != oidPrefixCur {
			// emit (oid[:6], oid[6:8]oid[6:8]...pos[0:6]pos[0:6]...)
			binary.BigEndian.PutUint64(oidb[:], uint64(oid))
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

		binary.BigEndian.PutUint64(oidb[:], uint64(oid))
		binary.BigEndian.PutUint64(posb[:], uint64(pos))

		// XXX check pos does not overflow 6 bytes
		oidBuf = append(oidBuf, oidb[6:8]...)
		posBuf = append(posBuf, posb[0:6]...)
	}

	e.Close()

skip:
	err = p.Encode(pickle.None{})

out:
	if err == nil {
		return err
	}

	if _, ok := err.(pikle.TypeError); ok {
		panic(err)	// all our types are expected to be supported by pickle
	}

	// otherwise it is an error returned by writer, which should already
	// have filename & op as context.
	return &IndexIOError{"save", err}
}

// LoadIndex loads index from a reader
func LoadIndex(r io.Reader) (topPos int64, fsi *fsIndex, err error) {
	p := pickle.NewDecoder(r)

	// if we can know file position we can show it in error context
	rseek, _ := r.(io.Seeker)
	var rpos int64
	decode := func() (interface{}, error) {
		if rseek != nil {
			rpos = rseek.Seek(...)	// XXX not ok as p buffers r internally
		}
	}

	xtopPos, err := p.Decode()
	if err != nil {
		// TODO err
	}
	topPos, ok := xtopPos.(int64)
	if !ok {
		// TODO err
	}

	fsi = &fsIndex{}	// TODO cmpFunc ...
	var oidb [8]byte
	var posb [8]byte

loop:
	for {
		xv, err := p.Decode()
		if err != nil {
			// TODO err
		}
		switch xv.(type) {
		default:
			// TODO err
			break

		case pickle.None:
			break loop

		case []interface{}:
			// so far ok
		}

		v := xv.([]interface{})
		if len(v) != 2 {
			// TODO err
		}

		xoidPrefixStr := v[0]
		oidPrefixStr, ok := xoidPrefixStr.(string)
		if !ok || len(oidPrefixStr) != 6 {
			// TODO
		}
		copy(oidb[:], oidPrefixStr)
		oidPrefix := zodb.Oid(binary.BigEndian.Uint64(oidb[:]))

		xkvStr := v[1]
		kvStr, ok := xkvStr.(string)
		if !ok || len(kvStr) % 8 != 0 {
			// TODO
		}

		kvBuf := mem.Bytes(kvStr)

		n := len(kvBuf) / 8
		oidBuf := kvBuf[:n*2]
		posBuf := kvBuf[n*2:]

		for i:=0; i<n; i++ {
			oid := zodb.Oid(binary.BigEndian.Uint16(oidBuf[i*2:]))
			oid |= oidPrefix
			copy(posb[2:], posBuf[i*6:])
			tid := zodb.Tid(binary.BigEndian.Uint64(posb[:]))

			fsi.Set(oid, tid)
		}
	}

	return topPos, fsi, nil

out:
	if err == nil {
		return topPos, fsi, err
	}

	rname := IOName(r)

	// same for file name
	rname, _ := r.(interface{ Name() string })

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
		return f.RemoteAddr.String()

	case *io.LimitedReader:
		return IOName(f.R)


	case *io.PipeReader:
		fallthrough
	case *io.PipeWriter:
		return "pipe"

	// XXX SectionReader MultiReader TeeReader

	// bufio.Reader bufio.Writer bufio.Scanner


	default:
		return ""
	}
}
