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

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/kylelemons/godebug/pretty"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

// tStorage implements read-only storage for cache testing
type tStorage struct {
	//txnv []tTxnRecord	// transactions;  .tid↑

	// oid -> [](.serial, .data)
	dataMap map[zodb.Oid][]tOidData	// with .serial↑
}

// data for oid for 1 revision
type tOidData struct {
	serial zodb.Tid
	data   []byte
	err    error    // e.g. io error
}

func (stor *tStorage) Load(xid zodb.Xid) (data []byte, serial zodb.Tid, err error) {
	//fmt.Printf("> load(%v)\n", xid)
	//defer func() { fmt.Printf("< %v, %v, %v\n", data, serial, err) }()
	tid := xid.Tid
	if !xid.TidBefore {
		tid++		// XXX overflow
	}

	datav := stor.dataMap[xid.Oid]
	if datav == nil {
		return nil, 0, &zodb.ErrOidMissing{xid.Oid}
	}

	// find max entry with .serial < tid
	n := len(datav)
	i := n - 1 - sort.Search(n, func(i int) bool {
		v := datav[n - 1 - i].serial < tid
		//fmt.Printf("@%d -> %v  (@%d; %v)\n", i, v, n - 1 -i, tid)
		return v
	})
	//fmt.Printf("i: %d  n: %d\n", i, n)
	if i == -1 {
		// tid < all .serial - no such transaction
		return nil, 0, &zodb.ErrXidMissing{xid}
	}

	// check we have exact match if it was loadSerial
	if !xid.TidBefore && datav[i].serial != xid.Tid {
		return nil, 0, &zodb.ErrXidMissing{xid}
	}

	s, e := datav[i].serial, datav[i].err
	if e != nil {
		s = 0 // obey protocol of returning 0 with error
	}
	return datav[i].data, s, e
}

var ioerr = errors.New("input/output error")

var tstor = &tStorage{
	dataMap: map[zodb.Oid][]tOidData{
		1: {
			{4, []byte("hello"), nil},
			{7, nil, ioerr},
			{9, []byte("world"), nil},
		},
	},
}

func xidlt(oid zodb.Oid, tid zodb.Tid) zodb.Xid {
	return zodb.Xid{Oid: oid, XTid: zodb.XTid{Tid: tid, TidBefore: true}}
}

func xideq(oid zodb.Oid, tid zodb.Tid) zodb.Xid {
	return zodb.Xid{Oid: oid, XTid: zodb.XTid{Tid: tid, TidBefore: false}}
}

func TestCache(t *testing.T) {
	// XXX hack; place=ok?
	pretty.CompareConfig.PrintStringers = true

	// XXX <100 <90 <80
	//	q<110	-> a) 110 <= cache.before   b) otherwise
	//	q<85	-> a) inside 90.serial..90  b) outside
	//
	// XXX cases when .serial=0 (not yet determined - 1st loadBefore is in progress)
	// XXX for every serial check before = (s-1, s, s+1)

	// merge: rcePrev + (rce + rceNext) ?

	tc := Checker{t}
	ok1 := func(v bool) { t.Helper(); tc.ok1(v) }
	//eq  := func(a, b interface{}) { t.Helper(); tc.assertEq(a, b) }

	hello := []byte("hello")
	world := []byte("world")

	c := NewCache(tstor)

	checkLoad := func(xid zodb.Xid, data []byte, serial zodb.Tid, err error) {
		t.Helper()
		bad := &bytes.Buffer{}
		d, s, e := c.Load(xid)
		if !reflect.DeepEqual(data, d) {
			fmt.Fprintf(bad, "data:\n%s\n", pretty.Compare(data, d))
		}
		if serial != s {
			fmt.Fprintf(bad, "serial:\n%s\n", pretty.Compare(serial, s))
		}
		if !reflect.DeepEqual(err, e) {
			fmt.Fprintf(bad, "err:\n%s\n", pretty.Compare(err, e))
		}

		if bad.Len() != 0 {
			t.Fatalf("load(%v):\n%s", xid, bad.Bytes())
		}
	}

	checkRCE := func(rce *revCacheEntry, before, serial zodb.Tid, data []byte, err error) {
		t.Helper()
		bad := &bytes.Buffer{}
		if rce.before != before {
			fmt.Fprintf(bad, "before:\n%s\n", pretty.Compare(before, rce.before))
		}
		if rce.serial != serial {
			fmt.Fprintf(bad, "serial:\n%s\n", pretty.Compare(serial, rce.serial))
		}
		if !reflect.DeepEqual(rce.data, data) {
			fmt.Fprintf(bad, "data:\n%s\n", pretty.Compare(data, rce.data))
		}
		if !reflect.DeepEqual(rce.err, err) {
			fmt.Fprintf(bad, "err:\n%s\n", pretty.Compare(err, rce.err))
		}

		if bad.Len() != 0 {
			t.Fatalf("rce:\n%s", bad.Bytes())	// XXX add oid?
		}
	}

	checkOCE := func(oid zodb.Oid, rcev ...*revCacheEntry) {
		t.Helper()
		oce := c.entryMap[oid]
		if !reflect.DeepEqual(oce.rcev, rcev) {
			t.Fatalf("oce(%v):\n%s", oid, pretty.Compare(rcev, oce.rcev))
		}
	}

	// load <3 -> new rce entry
	checkLoad(xidlt(1,3), nil, 0, &zodb.ErrXidMissing{xidlt(1,3)})
	oce1 := c.entryMap[1]
	ok1(len(oce1.rcev) == 1)
	rce1_b3 := oce1.rcev[0]
	checkRCE(rce1_b3, 3, 0, nil, &zodb.ErrXidMissing{xidlt(1,3)})

	// load <4 -> <3 merged with <4
	checkLoad(xidlt(1,4), nil, 0, &zodb.ErrXidMissing{xidlt(1,4)})
	ok1(len(oce1.rcev) == 1)
	rce1_b4 := oce1.rcev[0]
	ok1(rce1_b4 != rce1_b3) // rce1_b3 was merged into rce1_b4
	checkRCE(rce1_b4, 4, 0, nil, &zodb.ErrXidMissing{xidlt(1,4)})

	// load <2 -> <2 merged with <4
	checkLoad(xidlt(1,2), nil, 0, &zodb.ErrXidMissing{xidlt(1,2)})
	ok1(len(oce1.rcev) == 1)
	ok1(oce1.rcev[0] == rce1_b4)
	checkRCE(rce1_b4, 4, 0, nil, &zodb.ErrXidMissing{xidlt(1,4)})

	// load <6 -> new rce entry with data
	checkLoad(xidlt(1,6), hello, 4, nil)
	ok1(len(oce1.rcev) == 2)
	rce1_b6 := oce1.rcev[1]
	checkRCE(rce1_b6, 6, 4, hello, nil)
	checkOCE(1, rce1_b4, rce1_b6)

	// load <5 -> <5 merged with <6
	checkLoad(xidlt(1,5), hello, 4, nil)
	checkOCE(1, rce1_b4, rce1_b6)

	// load <7 -> <6 merged with <7
	checkLoad(xidlt(1,7), hello, 4, nil)
	ok1(len(oce1.rcev) == 2)
	rce1_b7 := oce1.rcev[1]
	ok1(rce1_b7 != rce1_b6)
	checkRCE(rce1_b7, 7, 4, hello, nil)
	checkOCE(1, rce1_b4, rce1_b7)

	// load <8 -> ioerr + new rce
	checkLoad(xidlt(1,8), nil, 0, ioerr)
	ok1(len(oce1.rcev) == 3)
	rce1_b8 := oce1.rcev[2]
	checkRCE(rce1_b8, 8, 0, nil, ioerr)
	checkOCE(1, rce1_b4, rce1_b7, rce1_b8)

	// load <9 -> ioerr + new rce (IO errors are not merged)
	checkLoad(xidlt(1,9), nil, 0, ioerr)
	ok1(len(oce1.rcev) == 4)
	rce1_b9 := oce1.rcev[3]
	checkRCE(rce1_b9, 9, 0, nil, ioerr)
	checkOCE(1, rce1_b4, rce1_b7, rce1_b8, rce1_b9)

	// load <10 -> new data rce, not merged with ioerr @<9
	checkLoad(xidlt(1,10), world, 9, nil)
	ok1(len(oce1.rcev) == 5)
	rce1_b10 := oce1.rcev[4]
	checkRCE(rce1_b10, 10, 9, world, nil)
	checkOCE(1, rce1_b4, rce1_b7, rce1_b8, rce1_b9, rce1_b10)

	// load <12 -> <10 merged with <12
	checkLoad(xidlt(1,12), world, 9, nil)
	ok1(len(oce1.rcev) == 5)
	rce1_b12 := oce1.rcev[4]
	ok1(rce1_b12 != rce1_b10)
	checkRCE(rce1_b12, 12, 9, world, nil)
	checkOCE(1, rce1_b4, rce1_b7, rce1_b8, rce1_b9, rce1_b12)

	// simulate case where <14 and <16 were loaded in parallel, both are ready
	// but <14 takes oce lock first before <16 and so <12 is not yet merged
	// with <16 -> <12 and <14 should be merged into <16.

	// (manually add rce1_b16 so it is not merged with <12)
	rce1_b16, new16 := c.lookupRCE(xidlt(1,16))
	ok1(new16)
	rce1_b16.serial = 9
	rce1_b16.data = world
	checkOCE(1, rce1_b4, rce1_b7, rce1_b8, rce1_b9, rce1_b12, rce1_b16)
	ok1(!rce1_b16.loaded())

	// (lookup <14 while <16 is not yet loaded so <16 is not picked
	//  automatically at lookup phase)
	rce1_b14, new14 := c.lookupRCE(xidlt(1,14))
	ok1(new14)
	checkOCE(1, rce1_b4, rce1_b7, rce1_b8, rce1_b9, rce1_b12, rce1_b14, rce1_b16)

	// (now <16 becomes ready but not yet takes oce lock)
	close(rce1_b16.ready)
	ok1(rce1_b16.loaded())
	checkOCE(1, rce1_b4, rce1_b7, rce1_b8, rce1_b9, rce1_b12, rce1_b14, rce1_b16)

	// (<14 also becomes ready and takes oce lock first, merging <12 and <14 into <16)
	c.loadRCE(rce1_b14, xidlt(1,14))
	checkRCE(rce1_b14, 14, 9, world, nil)
	checkRCE(rce1_b16, 16, 9, world, nil)
	checkRCE(rce1_b12, 12, 9, world, nil)
	checkOCE(1, rce1_b4, rce1_b7, rce1_b8, rce1_b9, rce1_b16)
}

type Checker struct {
	t *testing.T
}

func (c *Checker) ok1(v bool) {
	c.t.Helper()
	if !v {
		c.t.Fatal("!ok")
	}
}

func (c *Checker) assertEq(a, b interface{}) {
	c.t.Helper()
	if !reflect.DeepEqual(a, b) {
		c.t.Fatal("!eq:\n", pretty.Compare(a, b))
	}
}

/*
type tTxnRecord struct {
	tid	zodb.Tid

	// data records for oid changed in transaction
	// .oid↑
	datav []tDataRecord
}

type tDataRecord struct {
	oid	zodb.Oid
	data	[]byte
}

	if xid.TidBefore {
		// find max txn with .tid < xid.Tid
		n := len(s.txnv)
		i := n - 1 - sort.Search(n, func(i int) bool {
			return s.txnv[n - 1 - i].tid < xid.Tid
		})
		if i == -1 {
			// XXX xid.Tid < all .tid - no such transaction
		}
	}
*/
