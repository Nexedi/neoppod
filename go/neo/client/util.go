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

package client
// misc utilities

import (
	"bytes"
	"compress/zlib"
	"context"
	"io"

	"lab.nexedi.com/kirr/neo/go/xcommon/log"
)

// lclose closes c and logs closing error if there was any.
// the error is otherwise ignored
// XXX dup in neo,server
func lclose(ctx context.Context, c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Error(ctx, err)
	}
}

// decompress decompresses data according to zlib encoding.
//
// out buffer, if there is enough capacity, is used for decompression destination.
// if out has not enough capacity a new buffer is allocated and used.
//
// return: destination buffer with full decompressed data or error.
func decompress(in []byte, out []byte) (data []byte, err error) {
	bin := bytes.NewReader(in)
	zr, err := zlib.NewReader(bin)
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := zr.Close()
		if err2 != nil && err == nil {
			err = err2
			data = nil
		}
	}()

	bout := bytes.NewBuffer(out)
	_, err = io.Copy(bout, zr)
	if err != nil {
		return nil, err
	}

	return bout.Bytes(), nil
}
