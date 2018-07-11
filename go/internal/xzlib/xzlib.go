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

/* Czlib performs much faster, at least to decompress real data:

name                        old time/op    new time/op    delta
deco/unzlib/py/null-1K        2.12µs ± 1%    2.11µs ± 2%     ~     (p=0.841 n=5+5)
deco/unzlib/go/null-1K        1.89µs ± 1%    2.27µs ± 1%  +20.03%  (p=0.008 n=5+5)
deco/unzlib/py/null-4K        13.5µs ± 4%    13.3µs ± 0%     ~     (p=0.310 n=5+5)
deco/unzlib/go/null-4K        8.54µs ± 0%    8.91µs ± 0%   +4.43%  (p=0.008 n=5+5)
deco/unzlib/py/null-2M        5.20ms ±10%    5.31ms ± 1%     ~     (p=0.548 n=5+5)
deco/unzlib/go/null-2M        2.58ms ± 1%    3.87ms ± 0%  +50.13%  (p=0.008 n=5+5)
deco/unzlib/py/wczdata-avg    24.1µs ± 1%    23.9µs ± 0%     ~     (p=0.114 n=4+4)
deco/unzlib/go/wczdata-avg    68.0µs ± 1%    20.9µs ± 0%  -69.29%  (p=0.008 n=5+5)
deco/unzlib/py/wczdata-max    23.5µs ± 1%    23.5µs ± 0%     ~     (p=0.556 n=4+5)
deco/unzlib/go/wczdata-max    67.8µs ± 0%    20.7µs ± 1%  -69.45%  (p=0.008 n=5+5)
deco/unzlib/py/prod1-avg      4.47µs ± 2%    4.44µs ± 1%     ~     (p=0.341 n=5+5)
deco/unzlib/go/prod1-avg      11.0µs ± 0%     4.1µs ± 1%  -62.39%  (p=0.016 n=5+4)
deco/unzlib/py/prod1-max       326µs ± 0%     325µs ± 0%     ~     (p=0.095 n=5+5)
deco/unzlib/go/prod1-max       542µs ± 0%     262µs ± 0%  -51.71%  (p=0.008 n=5+5)
*/
	"github.com/DataDog/czlib"
)

// Compress compresses data according to zlib encoding.
//
// default level and dictionary are used.
func Compress(data []byte) (zdata []byte) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err := w.Write(data)
	if err != nil {
		panic(err) // bytes.Buffer.Write never return error
	}
	err = w.Close()
	if err != nil {
		panic(err) // ----//----
	}
	return b.Bytes()
}

// Decompress decompresses data according to zlib encoding.
//
// return: destination buffer with full decompressed data or error.
func Decompress(zdata []byte) (data []byte, err error) {
	return czlib.Decompress(zdata)
}

/*
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
func Decompress(zdata []byte, out []byte) (data []byte, err error) {
	bin := bytes.NewReader(zdata)
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

	bout := bytes.NewBuffer(out[:0])
	_, err = io.Copy(bout, zr)
	if err != nil {
		return nil, err
	}

	return bout.Bytes(), nil
}
*/
