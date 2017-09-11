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

package zodb
// data buffers management

import (
	"math/bits"
	"sync"

	"lab.nexedi.com/kirr/go123/xmath"
)

// Buf represents memory buffer.
//
// To lower pressure on Go garbage-collector allocate buffers with BufAlloc and
// free them with Buf.Free.
//
// Custom allocation functions affect only performance, not correctness -
// everything should work if data buffer is allocated and/or free'ed
// regular Go/GC-way.
type Buf struct {
	Data []byte
}

const order0 = 4 // buf sizes start from 2^4 (=16)

var bufPoolv = [14]sync.Pool{} // buf size stop at 2^(4+14-1) (=128K)


func init() {
	for i := 0; i < len(bufPoolv); i++ {
		bufPoolv[i].New = func() interface{} {
			// NOTE *Buf, not just buf, to avoid allocation when
			// making interface{} from it (interface{} wants to always point to heap)
			return &Buf{Data: make([]byte, 1 << (order0 + uint(i)))}
		}
	}
}

// BufAlloc allocates buffer of requested size from freelist.
//
// buffer memory is not initialized.
func BufAlloc(size int) *Buf {
	// order = min i: 2^i >= size
	order := xmath.CeilLog2(uint64(size))

	order -= order0
	if order < 0 {
		order = 0
	}

	// if too big - allocate straightly from heap
	if order >= len(bufPoolv) {
		return &Buf{Data: make([]byte, size)}
	}

	buf := bufPoolv[order].Get().(*Buf)
	buf.Data = buf.Data[:size] // leaving cap as is = 2^i
	return buf
}

// Free returns no-longer needed buf to freelist.
//
// The caller must not use buf after call to Free.
func (buf *Buf) Free() {
	// order = max i: 2^i <= cap
	order := bits.Len(uint(cap(buf.Data)))

	order -= order0
	if order < 0 {
		return // too small
	}

	if order >= len(bufPoolv) {
		return // too big
	}

	bufPoolv[order].Put(buf)
}
