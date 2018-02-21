// Copyright (C) 2017-2018  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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

// Package zlib provides convenience utilities to compress/decompress zlib data.
package xzlib

import (
	"bytes"
	"compress/zlib"
	"io"
	"sync"
)


// Compress compresses data according to zlib encoding.
//
// XXX default level is used, etc.
func Compress(data []byte) (zdata []byte) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err := w.Write(data)
	if err != nil {
		panic(err)	// bytes.Buffer.Write never return error
	}
	err = w.Close()
	if err != nil {
		panic(err)	// ----//----
	}
	return b.Bytes()
}


// ---- zlib.Reader pool ----
// (creating zlib.NewReader for every decompress has high overhead for not large blocks)

// znull is a small valid zlib stream.
// we need it to create new zlib readers under sync.Pool .
var znull = Compress(nil)

var zrPool = sync.Pool{New: func() interface{} {
	r, err := zlib.NewReader(bytes.NewReader(znull))
	if err != nil {
		panic(err) // must not happen - znull is valid stream
	}
	return r
}}

// interface actually implemented by what zlib.NewReader returns
type zlibReader interface {
	io.ReadCloser
	zlib.Resetter
}

func zlibNewReader(r io.Reader) (zlibReader, error) {
	zr := zrPool.Get().(zlibReader)
	err := zr.Reset(r, nil)
	if err != nil {
		zlibFreeReader(zr)
		return nil, err
	}
	return zr, nil
}

func zlibFreeReader(r zlibReader) {
	zrPool.Put(r)
}

// Decompress decompresses data according to zlib encoding.
//
// out buffer, if there is enough capacity, is used for decompression destination.
// if out has not enough capacity a new buffer is allocated and used.
//
// return: destination buffer with full decompressed data or error.
func Decompress(in []byte, out []byte) (data []byte, err error) {
	bin := bytes.NewReader(in)
	zr, err := zlibNewReader(bin)
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := zr.Close()
		if err2 != nil && err == nil {
			err = err2
			data = nil
		}
		zlibFreeReader(zr)
	}()

	bout := bytes.NewBuffer(out)
	_, err = io.Copy(bout, zr)
	if err != nil {
		return nil, err
	}

	return bout.Bytes(), nil
}
