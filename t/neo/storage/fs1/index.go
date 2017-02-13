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


// TODO Encode		(__getstate__)
// TODO Decode		(__setstate__)
// TODO ? save(pos, fname)
// TODO ? klass.load(fname)

/*
// __getitem__
func (fsi *fsIndex) Get(oid zodb.Oid) zodb.Tid {	// XXX ,ok ?
	tid, ok := fsi.oid2tid.Get(oid)
	return tid, ok
}
*/

// __setitem__ -> Set

// on-disk index format
// (changed in 2010 in https://github.com/zopefoundation/ZODB/commit/1bb14faf)
//
// pos     (?) (int or long - INT_TYPES)
// (oid[:6], v.toString)   # ._data[oid]-> v (fsBucket().toString())
// (oid[:6], v.toString)
// ...
// None
//
//
// fsBucket.toString():
// oid[6:8]oid[6:8]oid[6:8]...tid[0:6]tid[0:6]tid[0:6]...


const oidPrefixMask zodb.Oid = ((1<<64-1) ^ (1<<16 -1))	// 0xffffffffffff0000

// Save saves the index to a writer
// XXX proper error context
func (fsi *fsIndex) Save(fsPos int64, w io.Writer) error {
	p := pickle.NewEncoder(w)

	err := p.Encode(fsPos)
	if err != nil {
		return err
	}

	var oidb [8]byte
	var tidb [8]byte
	var oidPrefixCur zodb.Oid	// current oid[0:6] with [6:8] = 00
	oidBuf := []byte{}		// current oid[6:8]oid[6:8]...
	tidBuf := []byte{}		// current tid[0:6]tid[0:6]...
	var t [2]interface{}		// tuple for (oid, fsBucket.toString())


	e, err := fsi.SeekFirst()
	if err == io.EOF {
		goto skip	// empty btree
	}

	for {
		oid, tid, errStop := e.Next()
		oidPrefix := oid & oidPrefixMask

		if oidPrefix != oidPrefixCur {
			// emit pickle for current oid06
			binary.BigEndian.PutUint64(oidb[:], uint64(oid))
			t[0] = oidb[0:6]
			t[1] = bytes.Join([][]byte{oidBuf, tidBuf}, nil)
			err = p.Encode(t)
			if err != nil {
				return err
			}

			oidPrefixCur = oidPrefix
			oidBuf = oidBuf[:0]
			tidBuf = tidBuf[:0]
		}

		if errStop != nil {
			break
		}

		binary.BigEndian.PutUint64(oidb[:], uint64(oid))
		binary.BigEndian.PutUint64(tidb[:], uint64(tid))

		oidBuf = append(oidBuf, oidb[6:8]...)
		tidBuf = append(tidBuf, tidb[0:6]...)
	}

	e.Close()

skip:
	err = p.Encode(pickle.None{})
	return err
}

// LoadIndex loads index from a reader
func LoadIndex(r io.Reader) (fsPos int64, fsi *fsIndex, err error) {
	p := pickle.NewDecoder(r)

	// if we can know file position we can show it in error context
	rseek, _ := r.(io.Seeker)
	var rpos int64
	decode := func() (interface{}, error) {
		if rseek != nil {
			rpos = rseek.Seek(...)	// XXX not ok as p buffers r internally
		}
	}

	xpos, err := p.Decode()
	if err != nil {
		// TODO err
	}
	fsPos, ok := xpos.(int64)
	if !ok {
		// TODO err
	}

	fsi = &fsIndex{}	// TODO cmpFunc ...
	var oidb [8]byte
	var tidb [8]byte

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
		tidBuf := kvBuf[n*2:]

		for i:=0; i<n; i++ {
			oid := zodb.Oid(binary.BigEndian.Uint16(oidBuf[i*2:]))
			oid |= oidPrefix
			copy(tidb[2:], tidBuf[i*6:])
			tid := zodb.Tid(binary.BigEndian.Uint64(tidb[:]))

			fsi.Set(oid, tid)
		}
	}

	return fsPos, fsi, nil

xerror:
	// same for file name
	rname, _ := r.(interface{ Name() string })

}
