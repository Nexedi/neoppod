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

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"

	"lab.nexedi.com/kirr/go123/mem"

	"github.com/kylelemons/godebug/pretty"
)

// tStorage implements read-only storage for cache testing.
type tStorage struct {
	// oid -> [](.serial↑, .data)
	dataMap map[Oid][]tOidData
}

// data for oid for 1 revision.
type tOidData struct {
	serial Tid
	data   []byte
	err    error // e.g. io error
}

// create new buffer with specified content copied there.
func mkbuf(data []byte) *mem.Buf {
	buf := mem.BufAlloc(len(data))
	copy(buf.Data, data)
	return buf
}

// check whether buffers hold same data or both are nil.
//
// NOTE we ignore refcnt here
func bufSame(buf1, buf2 *mem.Buf) bool {
	if buf1 == nil {
		return (buf2 == nil)
	}

	return reflect.DeepEqual(buf1.Data, buf2.Data)
}

func (stor *tStorage) URL() string {
	return "test"
}

func (stor *tStorage) Load(_ context.Context, xid Xid) (buf *mem.Buf, serial Tid, err error) {
	//fmt.Printf("> load(%v)\n", xid)
	//defer func() { fmt.Printf("< %v, %v, %v\n", buf.XData(), serial, err) }()
	buf, serial, err = stor.load(xid)
	if err != nil {
		err = &OpError{URL: stor.URL(), Op: "load", Args: xid, Err: err}
	}
	return buf, serial, err
}

func (stor *tStorage) load(xid Xid) (buf *mem.Buf, serial Tid, err error) {
	datav := stor.dataMap[xid.Oid]
	if datav == nil {
		return nil, 0, &NoObjectError{xid.Oid}
	}

	// find max entry with .serial <= xid.At
	n := len(datav)
	i := n - 1 - sort.Search(n, func(i int) bool {
		v := datav[n - 1 - i].serial <= xid.At
		//fmt.Printf("@%d -> %v  (@%d; %v)\n", i, v, n - 1 -i, xid.At)
		return v
	})
	//fmt.Printf("i: %d  n: %d\n", i, n)
	if i == -1 {
		// xid.At < all .serial - no such transaction
		return nil, 0, &NoDataError{Oid: xid.Oid, DeletedAt: 0}
	}

	s, e := datav[i].serial, datav[i].err
	b := mkbuf(datav[i].data)
	if e != nil {
		b, s = nil, 0 // obey protocol of returning nil, 0 with error
	}
	return b, s, e
}

var ioerr = errors.New("input/output error")

func xidat(oid Oid, tid Tid) Xid {
	return Xid{Oid: oid, At: tid}
}

func nodata(oid Oid, deletedAt Tid) *NoDataError {
	return &NoDataError{Oid: oid, DeletedAt: deletedAt}
}

func TestCache(t *testing.T) {
	// XXX hack; place=ok?
	pretty.CompareConfig.PrintStringers = true
	debug := pretty.DefaultConfig
	debug.IncludeUnexported = true

	__ := Checker{t}
	ok1 := func(v bool) { t.Helper(); __.ok1(v) }

	hello := []byte("hello")
	world := []byte("world!!")
	zz    := []byte("zz")
	www   := []byte("www")
	big   := []byte("0123456789")

	tstor := &tStorage{
		dataMap: map[Oid][]tOidData{
			1: {
				{4, hello, nil},
				{7, nil, ioerr},
				{10, world, nil},
				{16, zz, nil},
				{20, www, nil},
				{77, big, nil},
			},
		},
	}

	b := mkbuf

	c := NewCache(tstor, 100 /* > Σ all data */)
	ctx := context.Background()

	checkLoad := func(xid Xid, buf *mem.Buf, serial Tid, errCause error) {
		t.Helper()
		bad := &bytes.Buffer{}
		b, s, e := c.Load(ctx, xid)
		if !bufSame(buf, b) {
			fmt.Fprintf(bad, "buf:\n%s\n", pretty.Compare(buf, b))
		}
		if serial != s {
			fmt.Fprintf(bad, "serial:\n%s\n", pretty.Compare(serial, s))
		}

		var err error
		if errCause != nil {
			err = &OpError{URL: "test", Op: "load", Args: xid, Err: errCause}
		}
		if !reflect.DeepEqual(err, e) {
			fmt.Fprintf(bad, "err:\n%s\n", pretty.Compare(err, e))
		}

		if bad.Len() != 0 {
			t.Fatalf("load(%v):\n%s", xid, bad.Bytes())
		}
	}

	checkOIDV := func(oidvOk ...Oid) {
		t.Helper()

		var oidv []Oid
		for oid := range c.entryMap {
			oidv = append(oidv, oid)
		}

		sort.Slice(oidv, func(i, j int) bool {
			return oidv[i] < oidv[j]
		})

		if !reflect.DeepEqual(oidv, oidvOk) {
			t.Fatalf("oidv: %s", pretty.Compare(oidvOk, oidv))
		}
	}

	checkRCE := func(rce *revCacheEntry, head, serial Tid, buf *mem.Buf, err error) {
		t.Helper()
		bad := &bytes.Buffer{}
		if rce.head != head {
			fmt.Fprintf(bad, "head:\n%s\n", pretty.Compare(head, rce.head))
		}
		if rce.serial != serial {
			fmt.Fprintf(bad, "serial:\n%s\n", pretty.Compare(serial, rce.serial))
		}
		if !bufSame(rce.buf, buf) {
			fmt.Fprintf(bad, "buf:\n%s\n", pretty.Compare(buf, rce.buf))
		}
		if !reflect.DeepEqual(rce.err, err) {
			fmt.Fprintf(bad, "err:\n%s\n", pretty.Compare(err, rce.err))
		}

		if bad.Len() != 0 {
			t.Fatalf("rce:\n%s", bad.Bytes()) // XXX add oid?
		}
	}

	checkOCE := func(oid Oid, rcev ...*revCacheEntry) {
		t.Helper()
		oce, ok := c.entryMap[oid]
		if !ok {
			t.Fatalf("oce(%v): not present in cache", oid)
		}
		oceRcev := oce.rcev
		if len(oceRcev) == 0 {
			oceRcev = nil // nil != []{}
		}
		if !reflect.DeepEqual(oceRcev, rcev) {
			t.Fatalf("oce(%v):\n%s\n", oid, pretty.Compare(rcev, oceRcev))
		}
	}

	checkMRU := func(sizeOk int, mruvOk ...*revCacheEntry) {
		t.Helper()
		size := 0
		var mruv []*revCacheEntry
		for hp, h := &c.lru, c.lru.Prev(); h != &c.lru; hp, h = h, h.Prev() {
			//xv := []interface{}{&c.lru, h.rceFromInLRU()}
			//debug.Print(xv)	// &c.lru, h.rceFromInLRU())
			if h.Next() != hp {
				t.Fatalf("LRU list .next/.prev broken for\nh:\n%s\n\nhp:\n%s\n",
					debug.Sprint(h), debug.Sprint(hp))
			}
			rce := h.rceFromInLRU()
			size += rce.buf.Len()
			mruv = append(mruv, rce)
		}
		if !reflect.DeepEqual(mruv, mruvOk) {
			t.Fatalf("MRU:\n%s\n", pretty.Compare(mruv, mruvOk))
		}

		if size != sizeOk {
			t.Fatalf("cache: size(all-rce-in-lru): %d  ; want: %d", size, sizeOk)
		}
		if size != c.size {
			t.Fatalf("cache: size(all-rce-in-lru): %d  ; c.size: %d", size, c.size)
		}
	}

	// ---- verify cache behaviour for must be loaded/merged entries ----
	// (this exercises mostly loadRCE/tryMerge)

	checkOIDV()
	checkMRU(0)

	// load @2 -> new rce entry
	checkLoad(xidat(1,2), nil, 0, nodata(1,0))
	checkOIDV(1)
	oce1 := c.entryMap[1]
	ok1(len(oce1.rcev) == 1)
	rce1_h2 := oce1.rcev[0]
	checkRCE(rce1_h2, 2, 0, nil, nodata(1,0))
	checkMRU(0, rce1_h2)

	// load @3 -> 2] merged with 3]
	checkLoad(xidat(1,3), nil, 0, nodata(1,0))
	checkOIDV(1)
	ok1(len(oce1.rcev) == 1)
	rce1_h3 := oce1.rcev[0]
	ok1(rce1_h3 != rce1_h2) // rce1_h2 was merged into rce1_h3
	checkRCE(rce1_h3, 3, 0, nil, nodata(1,0))
	checkMRU(0, rce1_h3)

	// load @1 -> 1] merged with 3]
	checkLoad(xidat(1,1), nil, 0, nodata(1,0))
	checkOIDV(1)
	ok1(len(oce1.rcev) == 1)
	ok1(oce1.rcev[0] == rce1_h3)
	checkRCE(rce1_h3, 3, 0, nil, nodata(1,0))
	checkMRU(0, rce1_h3)

	// load @5 -> new rce entry with data
	checkLoad(xidat(1,5), b(hello), 4, nil)
	checkOIDV(1)
	ok1(len(oce1.rcev) == 2)
	rce1_h5 := oce1.rcev[1]
	checkRCE(rce1_h5, 5, 4, b(hello), nil)
	checkOCE(1, rce1_h3, rce1_h5)
	checkMRU(5, rce1_h5, rce1_h3)

	// load @4 -> 4] merged with 5]
	checkLoad(xidat(1,4), b(hello), 4, nil)
	checkOIDV(1)
	checkOCE(1, rce1_h3, rce1_h5)
	checkMRU(5, rce1_h5, rce1_h3)

	// load @6 -> 5] merged with 6]
	checkLoad(xidat(1,6), b(hello), 4, nil)
	checkOIDV(1)
	ok1(len(oce1.rcev) == 2)
	rce1_h6 := oce1.rcev[1]
	ok1(rce1_h6 != rce1_h5)
	checkRCE(rce1_h6, 6, 4, b(hello), nil)
	checkOCE(1, rce1_h3, rce1_h6)
	checkMRU(5, rce1_h6, rce1_h3)

	// load @7 -> ioerr + new rce
	checkLoad(xidat(1,7), nil, 0, ioerr)
	checkOIDV(1)
	ok1(len(oce1.rcev) == 3)
	rce1_h7 := oce1.rcev[2]
	checkRCE(rce1_h7, 7, 0, nil, ioerr)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7)
	checkMRU(5, rce1_h7, rce1_h6, rce1_h3)

	// load @9 -> ioerr + new rce (IO errors are not merged)
	checkLoad(xidat(1,9), nil, 0, ioerr)
	checkOIDV(1)
	ok1(len(oce1.rcev) == 4)
	rce1_h9 := oce1.rcev[3]
	checkRCE(rce1_h9, 9, 0, nil, ioerr)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9)
	checkMRU(5, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// load @10 -> new data rce, not merged with ioerr at 9]
	checkLoad(xidat(1,10), b(world), 10, nil)
	checkOIDV(1)
	ok1(len(oce1.rcev) == 5)
	rce1_h10 := oce1.rcev[4]
	checkRCE(rce1_h10, 10, 10, b(world), nil)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h10)
	checkMRU(12, rce1_h10, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// load @11 -> 10] merged with 11]
	checkLoad(xidat(1,11), b(world), 10, nil)
	checkOIDV(1)
	ok1(len(oce1.rcev) == 5)
	rce1_h11 := oce1.rcev[4]
	ok1(rce1_h11 != rce1_h10)
	checkRCE(rce1_h11, 11, 10, b(world), nil)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h11)
	checkMRU(12, rce1_h11, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// simulate case where 13] (α) and 15] (β) were loaded in parallel, both are ready
	// but 13] (α) takes oce lock first before 15] and so 11] is not yet merged
	// with 15] -> 11] and 13] should be merged into 15].

	// (manually add rce1_h15 so it is not merged with 11])
	rce1_h15, new15 := c.lookupRCE(xidat(1,15), +0)
	ok1(new15)
	rce1_h15.serial = 10
	rce1_h15.buf = mkbuf(world)
	// here: first half of loadRCE(15]) before close(15].ready)
	checkOIDV(1)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h11, rce1_h15)
	ok1(!rce1_h15.loaded())
	checkMRU(12, rce1_h11, rce1_h9, rce1_h7, rce1_h6, rce1_h3) // no 15] yet

	// (lookup 13] while 15] is not yet loaded so 15] is not picked
	//  automatically at lookup phase)
	rce1_h13, new13 := c.lookupRCE(xidat(1,13), +0)
	ok1(new13)
	checkOIDV(1)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h11, rce1_h13, rce1_h15)
	checkMRU(12, rce1_h11, rce1_h9, rce1_h7, rce1_h6, rce1_h3) // no <14 and <16 yet

	// (now 15] becomes ready but does not yet takes oce lock)
	rce1_h15.waitBufRef = -1
	rce1_h15.ready.Done()
	ok1(rce1_h15.loaded())
	checkOIDV(1)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h11, rce1_h13, rce1_h15)
	checkMRU(12, rce1_h11, rce1_h9, rce1_h7, rce1_h6, rce1_h3) // no 13] and 15] yet

	// (13] also becomes ready and takes oce lock first, merging 11] and 13] into 15].
	//  15] did not yet took oce lock so c.size is temporarily reduced and
	//  15] is not yet on LRU list)
	c.loadRCE(ctx, rce1_h13)
	checkOIDV(1)
	checkRCE(rce1_h13, 13, 10, b(world), nil)
	checkRCE(rce1_h15, 15, 10, b(world), nil)
	checkRCE(rce1_h11, 11, 10, b(world), nil)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15)
	checkMRU(5 /*was 12*/, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// (15] takes oce lock and updates c.size and LRU list)
	rce1_h15.ready.Add(1) // so loadRCE could run
	c.loadRCE(ctx, rce1_h15)
	checkOIDV(1)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15)
	checkMRU(12, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// similar race in between 16] and 17] but now β (17]) takes oce lock first:

	rce1_h16, new16 := c.lookupRCE(xidat(1,16), +0)
	ok1(new16)
	rce1_h17, new17 := c.lookupRCE(xidat(1,17), +0)
	ok1(new17)

	// (16] loads but not yet takes oce lock)
	rce1_h16.serial = 16
	rce1_h16.buf = mkbuf(zz)
	rce1_h16.waitBufRef = -1
	rce1_h16.ready.Done()
	ok1(rce1_h16.loaded())
	checkOIDV(1)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15, rce1_h16, rce1_h17)
	checkMRU(12, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3) // no 16] and 17] yet

	// (17] loads and takes oce lock first - merge 16] with 17])
	c.loadRCE(ctx, rce1_h17)
	checkOIDV(1)
	checkRCE(rce1_h17, 17, 16, b(zz), nil)
	checkRCE(rce1_h16, 16, 16, b(zz), nil)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15, rce1_h17)
	checkMRU(14, rce1_h17, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// load @19 -> 17] merged with 19]
	checkLoad(xidat(1,19), b(zz), 16, nil)
	ok1(len(oce1.rcev) == 6)
	rce1_h19 := oce1.rcev[5]
	ok1(rce1_h19 != rce1_h17)
	checkOIDV(1)
	checkRCE(rce1_h19, 19, 16, b(zz), nil)
	checkOCE(1,  rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15, rce1_h19)
	checkMRU(14, rce1_h19, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// load @20 -> new 20]
	checkLoad(xidat(1,20), b(www), 20, nil)
	ok1(len(oce1.rcev) == 7)
	rce1_h20 := oce1.rcev[6]
	checkOIDV(1)
	checkRCE(rce1_h20, 20, 20, b(www), nil)
	checkOCE(1,  rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15, rce1_h19, rce1_h20)
	checkMRU(17, rce1_h20, rce1_h19, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// load @21 -> 20] merged with 21]
	checkLoad(xidat(1,21), b(www), 20, nil)
	ok1(len(oce1.rcev) == 7)
	rce1_h21 := oce1.rcev[6]
	ok1(rce1_h21 != rce1_h20)
	checkOIDV(1)
	checkRCE(rce1_h21, 21, 20, b(www), nil)
	checkOCE(1,  rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15, rce1_h19, rce1_h21)
	checkMRU(17, rce1_h21, rce1_h19, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3)


	// ---- verify rce lookup for must be cached entries ----
	// (this exercises lookupRCE)

	checkLookup := func(xid Xid, expect *revCacheEntry) {
		t.Helper()
		bad := &bytes.Buffer{}
		rce, rceNew := c.lookupRCE(xid, +0)
		if rceNew {
			fmt.Fprintf(bad, "rce must be already in cache\n")
		}
		if rce != expect {
			fmt.Fprintf(bad, "unexpected rce found:\n%s\n", pretty.Compare(expect, rce))
		}

		if bad.Len() != 0 {
			t.Fatalf("lookupRCE(%v):\n%s", xid, bad.Bytes())
		}
	}

	checkLookup(xidat(1,19), rce1_h19)
	checkLookup(xidat(1,18), rce1_h19)
	checkLookup(xidat(1,17), rce1_h19)
	checkLookup(xidat(1,16), rce1_h19)
	checkLookup(xidat(1,15), rce1_h15)
	checkLookup(xidat(1,14), rce1_h15)
	checkLookup(xidat(1,11), rce1_h15)
	checkLookup(xidat(1,10), rce1_h15)
	checkLookup(xidat(1,9), rce1_h9)

	// 8] must be separate from 7] and 9] because it is IO error there
	rce1_h8, new8 := c.lookupRCE(xidat(1,8), +0)
	ok1(new8)
	c.loadRCE(ctx, rce1_h8)
	checkOIDV(1)
	checkRCE(rce1_h8, 8, 0, nil, ioerr)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h8, rce1_h9, rce1_h15, rce1_h19, rce1_h21)
	checkMRU(17, rce1_h8, rce1_h21, rce1_h19, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	checkLookup(xidat(1,8), rce1_h8)
	checkLookup(xidat(1,7), rce1_h7)

	// have data exact and inexact hits
	checkLookup(xidat(1,7), rce1_h7)
	checkLookup(xidat(1,6), rce1_h6)
	checkLookup(xidat(1,5), rce1_h6)
	checkLookup(xidat(1,4), rce1_h6)

	// nodata exact and inexact hits
	checkLookup(xidat(1,3), rce1_h3)
	checkLookup(xidat(1,2), rce1_h3)
	checkLookup(xidat(1,1), rce1_h3)
	checkLookup(xidat(1,0), rce1_h3)

	// ---- verify how LRU changes for in-cache loads ----
	checkMRU(17, rce1_h8, rce1_h21, rce1_h19, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	checkLoad(xidat(1,6), b(hello), 4, nil)
	checkMRU(17, rce1_h6, rce1_h8, rce1_h21, rce1_h19, rce1_h15, rce1_h9, rce1_h7, rce1_h3)

	checkLoad(xidat(1,15), b(world), 10, nil)
	checkMRU(17, rce1_h15, rce1_h6, rce1_h8, rce1_h21, rce1_h19, rce1_h9, rce1_h7, rce1_h3)


	// ---- verify LRU eviction ----

	checkOIDV(1)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h8, rce1_h9, rce1_h15, rce1_h19, rce1_h21)
	checkMRU(17, rce1_h15, rce1_h6, rce1_h8, rce1_h21, rce1_h19, rce1_h9, rce1_h7, rce1_h3)

	c.SetSizeMax(16) // < c.size by 1 -> should trigger gc

	// evicted:
	// - 3]  (lru.1, nodata, size=0)	XXX ok to evict nodata & friends?
	// - 7]  (lru.2, ioerr, size=0)
	// - 9]  (lru.3, ioerr, size=0)
	// - 19] (lru.4, zz, size=2)
	checkOIDV(1)
	checkOCE(1,  rce1_h6, rce1_h8, rce1_h15, rce1_h21)
	checkMRU(15, rce1_h15, rce1_h6, rce1_h8, rce1_h21)

	// reload 19] -> 21] should be evicted
	c.Load(ctx, xidat(1,19))

	// - evicted 21] (lru.1, www, size=3)
	// - loaded  19] (zz, size=2)
	ok1(len(oce1.rcev) == 4)
	rce1_h19_2 := oce1.rcev[3]
	ok1(rce1_h19_2 != rce1_h19)
	checkOIDV(1)
	checkRCE(rce1_h19_2, 19, 16, b(zz), nil)
	checkOCE(1,  rce1_h6, rce1_h8, rce1_h15, rce1_h19_2)
	checkMRU(14, rce1_h19_2, rce1_h15, rce1_h6, rce1_h8)

	// load big 77] -> several rce must be evicted
	c.Load(ctx, xidat(1,77))

	// - evicted  8] (lru.1, ioerr, size=0)
	// - evicted  6] (lru.2, hello, size=5)
	// - evicted 15] (lru.3, world, size=7)
	// - loaded  77] (big, size=10)
	ok1(len(oce1.rcev) == 2)
	rce1_h77 := oce1.rcev[1]
	checkOIDV(1)
	checkRCE(rce1_h77, 77, 77, b(big), nil)
	checkOCE(1,  rce1_h19_2, rce1_h77)
	checkMRU(12, rce1_h77, rce1_h19_2)

	// sizeMax=0 evicts everything from cache
	c.SetSizeMax(0)
	checkOIDV()
	checkMRU(0)

	// and still loading works (because even if though rce's are evicted
	// they stay live while someone user waits and uses it)

	checkLoad(xidat(1,4), b(hello), 4, nil)
	checkOIDV()
	checkMRU(0)

	// ---- Load vs concurrent GC ----

	// in the following scenario if GC runs after Load completed lookupRCE
	// but before Load increfs returned buf, the GC will actually return
	// the buf to buffer pool and so Load will be returning wrong buffer:
	//
	// ---- 8< ----
	// T1			Tgc
	// Prefetch:
	//   RCELookedUp
	//   RCELoaded
	//			# GC - on hold
	// Load
	//   RCELookedUp
	//   -> pause T1
	//			# GC - unpause
	//			GCStart
	//			GCStop
	//   <- unpause T1
	// # load completes
	// ---- 8< ----
	//
	// it is hard to check this via stable tracepoints because, if done so,
	// after the problem is fixed the test will deadlock.
	// So test it probabilistically instead.
	c.SetSizeMax(0) // we want to GC to be constantly running
	for i := 0; i < 1e4; i++ {
		// issue Prefetch: this should create RCE and spawn loadRCE for it
		c.Prefetch(ctx, xidat(1,4))

		// issue Load: this should lookup the RCE and wait for it to be loaded.
		// if GC runs in parallel to Load there are chances it will
		// be running in between Load->lookupRCE and final rce.buf.XIncref()
		//
		// if something is incorrect with refcounting either
		// buf.Incref() in Load or buf.Release() in GC will panic.
		checkLoad(xidat(1,4), b(hello), 4, nil)
	}


	// XXX verify caching vs ctx cancel
	// XXX verify db inconsistency checks
	// XXX verify loading with before > cache.before
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

// ----------------------------------------

// noopStorage is dummy Loader which for any oid/xid always returns 1-byte data.
type noopStorage struct{}
var noopData = []byte{0}

func (s *noopStorage) URL() string {
	return "noop"
}

func (s *noopStorage) Load(_ context.Context, xid Xid) (buf *mem.Buf, serial Tid, err error) {
	return mkbuf(noopData), 1, nil
}

// benchLoad serially benchmarks a Loader - either storage directly or a cache on top of it.
//
// oid accessed are [0, worksize)
func benchLoad(b *testing.B, l Loader, worksize int) {
	benchLoadN(b, b.N, l, worksize)
}

// worker for benchLoad, with n overridding b.N
func benchLoadN(b *testing.B, n int, l Loader, worksize int) {
	ctx := context.Background()

	xid := Xid{At: 1, Oid: 0}
	for i := 0; i < n; i++ {
		buf, _, err := l.Load(ctx, xid)
		if err != nil {
			b.Fatal(err)
		}
		buf.XRelease()

		xid.Oid++
		if xid.Oid >= Oid(worksize) {
			xid.Oid = 0
		}
	}
}


// benchmark storage under cache
func BenchmarkNoopStorage(b *testing.B) { benchLoad(b, &noopStorage{}, b.N /* = ∞ */) }

// cache sizes to benchmark (elements = bytes (we are using 1-byte element))
var cachesizev = []int{0, 16, 128, 512, 4096}

// benchEachCache runs benchmark f against caches with various sizes on top of noop storage
func benchEachCache(b *testing.B, f func(b *testing.B, c *Cache)) {
	s := &noopStorage{}
	for _, size := range cachesizev {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			c := NewCache(s, size)
			f(b, c)
		})
	}
}

// benchmark cache while N(access) < N(cache-entries)
func BenchmarkCacheStartup(b *testing.B) {
	s := &noopStorage{}
	c := NewCache(s, b.N)
	benchLoad(b, c, b.N)
	b.StopTimer()
}

// Serially benchmark cache overhead - the additional time cache adds for loading not-yet-cached entries.
// cache is already started - N(access) > N(cache-entries).
func BenchmarkCacheNoHit(b *testing.B) {
	benchEachCache(b, func(b *testing.B, c *Cache) {
		benchLoad(b, c, b.N /* = ∞ */)
	})
}

// Serially benchmark t when load request hits cache.
// cache is already started - N(access) > N(cache-entries)
func BenchmarkCacheHit(b *testing.B) {
	benchEachCache(b, func(b *testing.B, c *Cache) {
		// warmup - load for cache size
		benchLoadN(b, c.sizeMax, c, c.sizeMax)

		b.ResetTimer()
		benchLoad(b, c, c.sizeMax)
	})
}

// ---- parallel benchmarks (many requests to 1 cache) ----

// benchLoadPar is like benchLoad but issues loads in parallel
func benchLoadPar(b *testing.B, l Loader, worksize int) {
	ctx := context.Background()
	np := runtime.GOMAXPROCS(0)
	p := uint64(0)

	b.RunParallel(func(pb *testing.PB) {
		oid0 := Oid(atomic.AddUint64(&p, +1)) // all workers start/iterate at different oid
		xid := Xid{At: 1, Oid: oid0}
		for pb.Next() {
			buf, _, err := l.Load(ctx, xid)
			if err != nil {
				b.Fatal(err)
			}
			buf.XRelease()

			xid.Oid += Oid(np)
			if xid.Oid >= Oid(worksize) {
				xid.Oid = oid0
			}
		}
	})
}

func BenchmarkNoopStoragePar(b *testing.B) { benchLoadPar(b, &noopStorage{}, b.N /* = ∞ */) }

func BenchmarkCacheStartupPar(b *testing.B) {
	s := &noopStorage{}
	c := NewCache(s, b.N)
	benchLoadPar(b, c, b.N)
	b.StopTimer()
}

func BenchmarkCacheNoHitPar(b *testing.B) {
	benchEachCache(b, func(b *testing.B, c *Cache) {
		benchLoadPar(b, c, b.N /* = ∞ */)
	})
}

func BenchmarkCacheHitPar(b *testing.B) {
	benchEachCache(b, func(b *testing.B, c *Cache) {
		// warmup (serially) - load for cache size
		benchLoadN(b, c.sizeMax, c, c.sizeMax)

		b.ResetTimer()
		benchLoadPar(b, c, c.sizeMax)
	})
}

// ---- parallel benchmarks (many caches - each is handled serially, as if each is inside separate process) ----
// XXX gc process is still only 1 shared.
// XXX this benchmark part will probably go away

// benchLoadProc is like benchLoad but works with PB, not B
func benchLoadProc(pb *testing.PB, l Loader, worksize int) error {
	ctx := context.Background()

	xid := Xid{At: 1, Oid: 0}
	for pb.Next() {
		buf, _, err := l.Load(ctx, xid)
		if err != nil {
			return err
		}
		buf.XRelease()

		xid.Oid++
		if xid.Oid >= Oid(worksize) {
			xid.Oid = 0
		}
	}

	return nil
}

func BenchmarkNoopStorageProc(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		s := &noopStorage{}
		err := benchLoadProc(pb, s, b.N)
		if err != nil {
			b.Fatal(err)
		}
	})
}

func BenchmarkCacheStartupProc(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		s := &noopStorage{}
		c := NewCache(s, b.N)
		err := benchLoadProc(pb, c, b.N)
		if err != nil {
			b.Fatal(err)
		}
		// XXX stop timer
	})
}

func benchEachCacheProc(b *testing.B, f func(b *testing.B, pb *testing.PB, c *Cache) error) {
	for _, size := range cachesizev {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				s := &noopStorage{}
				c := NewCache(s, size)
				err := f(b, pb, c)
				if err != nil {
					b.Fatal(err)
				}
			})
		})
	}
}

func BenchmarkCacheNoHitProc(b *testing.B) {
	benchEachCacheProc(b, func(b *testing.B, pb *testing.PB, c *Cache) error {
		return benchLoadProc(pb, c, b.N)
	})
}

func BenchmarkCacheHitProc(b *testing.B) {
	benchEachCacheProc(b, func(b *testing.B, pb *testing.PB, c *Cache) error {
		// XXX no warmup
		return benchLoadProc(pb, c, c.sizeMax)
	})
}
