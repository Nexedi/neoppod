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
	"sync"
	"sync/atomic"

	"lab.nexedi.com/kirr/go123/xmath"
)

// Buf is reference-counted memory buffer.
//
// To lower pressure on Go garbage-collector allocate buffers with BufAlloc and
// free them with Buf.Release.
//
// Custom allocation functions affect only performance, not correctness -
// everything should work if data buffer is allocated and/or free'ed
// via regular Go/GC-way.
type Buf struct {
	Data []byte

	// reference counter.
	//
	// NOTE to allow both Bufs created via BufAlloc and via std new, Buf is
	// created with refcnt=0. The real number of references to Buf is thus .refcnt+1
	refcnt int32
}

const order0 = 4 // buf sizes start from 2^4 (=16)

var bufPoolv = [19]sync.Pool{} // buf size stop at 2^(4+19-1) (=4M)


func init() {
	for i := 0; i < len(bufPoolv); i++ {
		i := i
		bufPoolv[i].New = func() interface{} {
			// NOTE *Buf, not just buf, to avoid allocation when
			// making interface{} from it (interface{} wants to always point to heap)
			return &Buf{Data: make([]byte, 1<<(order0+uint(i)))}
		}
	}
}

// BufAlloc allocates buffer of requested size from freelist.
//
// buffer memory is not initialized.
func BufAlloc(size int) *Buf {
	return BufAlloc64(int64(size))
}

// BufAlloc64 is same as BufAlloc but accepts int64 for size.
func BufAlloc64(size int64) *Buf {
	if size < 0 {
		panic("invalid size")
	}

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
	buf.refcnt = 0
	return buf
}

// Release marks buf as no longer used by caller.
//
// It decrements buf reference-counter and if it reaches zero returns buf to
// freelist.
//
// The caller must not use buf after call to Release.
//
// XXX naming? -> Free? -> Decref? -> Unref?
func (buf *Buf) Release() {
	rc := atomic.AddInt32(&buf.refcnt, -1)
	if rc < -1 {
		panic("Buf: refcnt < 0")
	}
	if rc > -1 {
		return
	}

	// order = max i: 2^i <= cap
	order := xmath.FloorLog2(uint64(cap(buf.Data)))

	order -= order0
	if order < 0 {
		return // too small
	}

	if order >= len(bufPoolv) {
		return // too big
	}

	bufPoolv[order].Put(buf)
}

// Incref increments buf's reference counter by 1.
func (buf *Buf) Incref() {
	atomic.AddInt32(&buf.refcnt, +1)
}

// XRelease releases buf it is != nil.
func (buf *Buf) XRelease() {
	if buf != nil {
		buf.Release()
	}
}

// XIncref increments buf's reference counter by 1 if buf != nil.
func (buf *Buf) XIncref() {
	if buf != nil {
		buf.Incref()
	}
}


// Len returns buf's len.
//
// it works even if buf=nil similarly to len() on nil []byte slice.
func (buf *Buf) Len() int {
	if buf != nil {
		return len(buf.Data)
	}
	return 0
}

// Cap returns buf's cap.
//
// it works even if buf=nil similarly to len() on nil []byte slice.
func (buf *Buf) Cap() int {
	if buf != nil {
		return cap(buf.Data)
	}
	return 0
}

// XData return's buf.Data or nil if buf == nil.
func (buf *Buf) XData() []byte {
	if buf != nil {
		return buf.Data
	}
	return nil
}
