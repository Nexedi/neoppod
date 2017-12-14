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

package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/kylelemons/godebug/pretty"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/kirr/go123/tracing"
	"lab.nexedi.com/kirr/neo/go/xcommon/xtesting"
)

// tStorage implements read-only storage for cache testing
type tStorage struct {
	// oid -> [](.serial↑, .data)
	dataMap map[zodb.Oid][]tOidData
}

// data for oid for 1 revision
type tOidData struct {
	serial zodb.Tid
	data   []byte
	err    error    // e.g. io error
}

// create new buffer with specified content copied there.
func mkbuf(data []byte) *zodb.Buf {
	buf := zodb.BufAlloc(len(data))
	copy(buf.Data, data)
	return buf
}

// check whether buffers hold same data or both are nil.
//
// NOTE we ignore refcnt here
func bufSame(buf1, buf2 *zodb.Buf) bool {
	if buf1 == nil {
		return (buf2 == nil)
	}

	return reflect.DeepEqual(buf1.Data, buf2.Data)
}

func (stor *tStorage) Load(_ context.Context, xid zodb.Xid) (buf *zodb.Buf, serial zodb.Tid, err error) {
	//fmt.Printf("> load(%v)\n", xid)
	//defer func() { fmt.Printf("< %v, %v, %v\n", buf.XData(), serial, err) }()

	datav := stor.dataMap[xid.Oid]
	if datav == nil {
		return nil, 0, &zodb.ErrOidMissing{xid.Oid}
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
		return nil, 0, &zodb.ErrXidMissing{xid}
	}

	s, e := datav[i].serial, datav[i].err
	if e != nil {
		s = 0 // obey protocol of returning 0 with error
	}
	return mkbuf(datav[i].data), s, e
}

var ioerr = errors.New("input/output error")

func xidat(oid zodb.Oid, tid zodb.Tid) zodb.Xid {
	return zodb.Xid{Oid: oid, At: tid}
}

// tracer which collects tracing events from all needed-for-tests sources
type tTracer struct {
	*xtesting.SyncTracer
}

type evCacheGCStart struct {
	c *Cache
}
func (t *tTracer) traceCacheGCStart(c *Cache)	{ t.Trace1(&evCacheGCStart{c}) }

type evCacheGCFinish struct {
	c *Cache
}
func (t *tTracer) traceCacheGCFinish(c *Cache)	{ t.Trace1(&evCacheGCFinish{c}) }

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
		dataMap: map[zodb.Oid][]tOidData{
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

	checkLoad := func(xid zodb.Xid, buf *zodb.Buf, serial zodb.Tid, err error) {
		t.Helper()
		bad := &bytes.Buffer{}
		b, s, e := c.Load(ctx, xid)
		if !bufSame(buf, b) {
			fmt.Fprintf(bad, "buf:\n%s\n", pretty.Compare(buf, b))
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

	checkRCE := func(rce *revCacheEntry, head, serial zodb.Tid, buf *zodb.Buf, err error) {
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
			t.Fatalf("rce:\n%s", bad.Bytes())	// XXX add oid?
		}
	}

	checkOCE := func(oid zodb.Oid, rcev ...*revCacheEntry) {
		t.Helper()
		oce := c.entryMap[oid]
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
	// (this excercises mostly loadRCE/tryMerge)

	checkMRU(0)

	// load @2 -> new rce entry
	checkLoad(xidat(1,2), nil, 0, &zodb.ErrXidMissing{xidat(1,2)})
	oce1 := c.entryMap[1]
	ok1(len(oce1.rcev) == 1)
	rce1_h2 := oce1.rcev[0]
	checkRCE(rce1_h2, 2, 0, nil, &zodb.ErrXidMissing{xidat(1,2)})
	checkMRU(0, rce1_h2)

	// load @3 -> 2] merged with 3]
	checkLoad(xidat(1,3), nil, 0, &zodb.ErrXidMissing{xidat(1,3)})
	ok1(len(oce1.rcev) == 1)
	rce1_h3 := oce1.rcev[0]
	ok1(rce1_h3 != rce1_h2) // rce1_h2 was merged into rce1_h3
	checkRCE(rce1_h3, 3, 0, nil, &zodb.ErrXidMissing{xidat(1,3)})
	checkMRU(0, rce1_h3)

	// load @1 -> 1] merged with 3]
	checkLoad(xidat(1,1), nil, 0, &zodb.ErrXidMissing{xidat(1,1)})
	ok1(len(oce1.rcev) == 1)
	ok1(oce1.rcev[0] == rce1_h3)
	checkRCE(rce1_h3, 3, 0, nil, &zodb.ErrXidMissing{xidat(1,3)})
	checkMRU(0, rce1_h3)

	// load @5 -> new rce entry with data
	checkLoad(xidat(1,5), b(hello), 4, nil)
	ok1(len(oce1.rcev) == 2)
	rce1_h5 := oce1.rcev[1]
	checkRCE(rce1_h5, 5, 4, b(hello), nil)
	checkOCE(1, rce1_h3, rce1_h5)
	checkMRU(5, rce1_h5, rce1_h3)

	// load @4 -> 4] merged with 5]
	checkLoad(xidat(1,4), b(hello), 4, nil)
	checkOCE(1, rce1_h3, rce1_h5)
	checkMRU(5, rce1_h5, rce1_h3)

	// load @6 -> 5] merged with 6]
	checkLoad(xidat(1,6), b(hello), 4, nil)
	ok1(len(oce1.rcev) == 2)
	rce1_h6 := oce1.rcev[1]
	ok1(rce1_h6 != rce1_h5)
	checkRCE(rce1_h6, 6, 4, b(hello), nil)
	checkOCE(1, rce1_h3, rce1_h6)
	checkMRU(5, rce1_h6, rce1_h3)

	// load @7 -> ioerr + new rce
	checkLoad(xidat(1,7), nil, 0, ioerr)
	ok1(len(oce1.rcev) == 3)
	rce1_h7 := oce1.rcev[2]
	checkRCE(rce1_h7, 7, 0, nil, ioerr)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7)
	checkMRU(5, rce1_h7, rce1_h6, rce1_h3)

	// load @9 -> ioerr + new rce (IO errors are not merged)
	checkLoad(xidat(1,9), nil, 0, ioerr)
	ok1(len(oce1.rcev) == 4)
	rce1_h9 := oce1.rcev[3]
	checkRCE(rce1_h9, 9, 0, nil, ioerr)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9)
	checkMRU(5, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// load @10 -> new data rce, not merged with ioerr at 9]
	checkLoad(xidat(1,10), b(world), 10, nil)
	ok1(len(oce1.rcev) == 5)
	rce1_h10 := oce1.rcev[4]
	checkRCE(rce1_h10, 10, 10, b(world), nil)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h10)
	checkMRU(12, rce1_h10, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// load @11 -> 10] merged with 11]
	checkLoad(xidat(1,11), b(world), 10, nil)
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
	rce1_h15, new15 := c.lookupRCE(xidat(1,15))
	ok1(new15)
	rce1_h15.serial = 10
	rce1_h15.buf = mkbuf(world)
	// here: first half of loadRCE(15]) before close(15].ready)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h11, rce1_h15)
	ok1(!rce1_h15.loaded())
	checkMRU(12, rce1_h11, rce1_h9, rce1_h7, rce1_h6, rce1_h3) // no 15] yet

	// (lookup 13] while 15] is not yet loaded so 15] is not picked
	//  automatically at lookup phase)
	rce1_h13, new13 := c.lookupRCE(xidat(1,13))
	ok1(new13)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h11, rce1_h13, rce1_h15)
	checkMRU(12, rce1_h11, rce1_h9, rce1_h7, rce1_h6, rce1_h3) // no <14 and <16 yet

	// (now 15] becomes ready but not yet takes oce lock)
	close(rce1_h15.ready)
	ok1(rce1_h15.loaded())
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h11, rce1_h13, rce1_h15)
	checkMRU(12, rce1_h11, rce1_h9, rce1_h7, rce1_h6, rce1_h3) // no 13] and 15] yet

	// (13] also becomes ready and takes oce lock first, merging 11] and 13] into 15].
	//  15] did not yet took oce lock so c.size is temporarily reduced and
	//  15] is not yet on LRU list)
	c.loadRCE(ctx, rce1_h13, 1)
	checkRCE(rce1_h13, 13, 10, b(world), nil)
	checkRCE(rce1_h15, 15, 10, b(world), nil)
	checkRCE(rce1_h11, 11, 10, b(world), nil)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15)
	checkMRU(5 /*was 12*/, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// (15] takes oce lock and updates c.size and LRU list)
	rce1_h15.ready = make(chan struct{}) // so loadRCE could run
	c.loadRCE(ctx, rce1_h15, 1)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15)
	checkMRU(12, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// similar race in between 16] and 17] but now β (17]) takes oce lock first:

	rce1_h16, new16 := c.lookupRCE(xidat(1,16))
	ok1(new16)
	rce1_h17, new17 := c.lookupRCE(xidat(1,17))
	ok1(new17)

	// (16] loads but not yet takes oce lock)
	rce1_h16.serial = 16
	rce1_h16.buf = mkbuf(zz)
	close(rce1_h16.ready)
	ok1(rce1_h16.loaded())
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15, rce1_h16, rce1_h17)
	checkMRU(12, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3) // no 16] and 17] yet

	// (17] loads and takes oce lock first - merge 16] with 17])
	c.loadRCE(ctx, rce1_h17, 1)
	checkRCE(rce1_h17, 17, 16, b(zz), nil)
	checkRCE(rce1_h16, 16, 16, b(zz), nil)
	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15, rce1_h17)
	checkMRU(14, rce1_h17, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// load @19 -> 17] merged with 19]
	checkLoad(xidat(1,19), b(zz), 16, nil)
	ok1(len(oce1.rcev) == 6)
	rce1_h19 := oce1.rcev[5]
	ok1(rce1_h19 != rce1_h17)
	checkRCE(rce1_h19, 19, 16, b(zz), nil)
	checkOCE(1,  rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15, rce1_h19)
	checkMRU(14, rce1_h19, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// load @20 -> new 20]
	checkLoad(xidat(1,20), b(www), 20, nil)
	ok1(len(oce1.rcev) == 7)
	rce1_h20 := oce1.rcev[6]
	checkRCE(rce1_h20, 20, 20, b(www), nil)
	checkOCE(1,  rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15, rce1_h19, rce1_h20)
	checkMRU(17, rce1_h20, rce1_h19, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3)

	// load @21 -> 20] merged with 21]
	checkLoad(xidat(1,21), b(www), 20, nil)
	ok1(len(oce1.rcev) == 7)
	rce1_h21 := oce1.rcev[6]
	ok1(rce1_h21 != rce1_h20)
	checkRCE(rce1_h21, 21, 20, b(www), nil)
	checkOCE(1,  rce1_h3, rce1_h6, rce1_h7, rce1_h9, rce1_h15, rce1_h19, rce1_h21)
	checkMRU(17, rce1_h21, rce1_h19, rce1_h15, rce1_h9, rce1_h7, rce1_h6, rce1_h3)


	// ---- verify rce lookup for must be cached entries ----
	// (this excersizes lookupRCE)

	checkLookup := func(xid zodb.Xid, expect *revCacheEntry) {
		t.Helper()
		bad := &bytes.Buffer{}
		rce, rceNew := c.lookupRCE(xid)
		if rceNew {
			fmt.Fprintf(bad, "rce must be already in cache\n")
		}
		if rce != expect {
			fmt.Fprintf(bad, "unexpected rce found:\n%s\n", pretty.Compare(expect, rce))
		}

		if bad.Len() != 0{
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
	rce1_h8, new8 := c.lookupRCE(xidat(1,8))
	ok1(new8)
	c.loadRCE(ctx, rce1_h8, 1)
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

	// (attach to Cache GC tracepoints)
	tracer := &tTracer{xtesting.NewSyncTracer()}
	pg := &tracing.ProbeGroup{}
	defer pg.Done()

	tracing.Lock()
	traceCacheGCStart_Attach(pg, tracer.traceCacheGCStart)
	traceCacheGCFinish_Attach(pg, tracer.traceCacheGCFinish)
	tracing.Unlock()

	// trace-checker for the events
	tc := xtesting.NewTraceChecker(t, tracer.SyncTracer)


	gcstart  := &evCacheGCStart{c}
	gcfinish := &evCacheGCFinish{c}

	checkOCE(1, rce1_h3, rce1_h6, rce1_h7, rce1_h8, rce1_h9, rce1_h15, rce1_h19, rce1_h21)
	checkMRU(17, rce1_h15, rce1_h6, rce1_h8, rce1_h21, rce1_h19, rce1_h9, rce1_h7, rce1_h3)

	go c.SetSizeMax(16) // < c.size by 1 -> should trigger gc
	tc.Expect(gcstart, gcfinish)

	// evicted:
	// - 3]  (lru.1, nodata, size=0)	XXX ok to evict nodata & friends?
	// - 7]  (lru.2, ioerr, size=0)
	// - 9]  (lru.3, ioerr, size=0)
	// - 19] (lru.4, zz, size=2)
	checkOCE(1,  rce1_h6, rce1_h8, rce1_h15, rce1_h21)
	checkMRU(15, rce1_h15, rce1_h6, rce1_h8, rce1_h21)

	// reload 19] -> 21] should be evicted
	go c.Load(ctx, xidat(1,19))
	tc.Expect(gcstart, gcfinish)

	// - evicted 21] (lru.1, www, size=3)
	// - loaded  19] (zz, size=2)
	ok1(len(oce1.rcev) == 4)
	rce1_h19_2 := oce1.rcev[3]
	ok1(rce1_h19_2 != rce1_h19)
	checkRCE(rce1_h19_2, 19, 16, b(zz), nil)
	checkOCE(1,  rce1_h6, rce1_h8, rce1_h15, rce1_h19_2)
	checkMRU(14, rce1_h19_2, rce1_h15, rce1_h6, rce1_h8)

	// load big 77] -> several rce must be evicted
	go c.Load(ctx, xidat(1,77))
	tc.Expect(gcstart, gcfinish)

	// - evicted  8] (lru.1, ioerr, size=0)
	// - evicted  6] (lru.2, hello, size=5)
	// - evicted 15] (lru.3, world, size=7)
	// - loaded  77] (big, size=10)
	ok1(len(oce1.rcev) == 2)
	rce1_h77 := oce1.rcev[1]
	checkRCE(rce1_h77, 77, 77, b(big), nil)
	checkOCE(1,  rce1_h19_2, rce1_h77)
	checkMRU(12, rce1_h77, rce1_h19_2)

	// sizeMax=0 evicts everything from cache
	go c.SetSizeMax(0)
	tc.Expect(gcstart, gcfinish)
	checkOCE(1)
	checkMRU(0)

	// and still loading works (because even if though rce's are evicted
	// they stay live while someone user waits and uses it)

	checkLoad(xidat(1,4), b(hello), 4, nil)
	tc.Expect(gcstart, gcfinish)
	checkOCE(1)
	checkMRU(0)


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
