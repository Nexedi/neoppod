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

// +build ignore

// zhash - compute hash of whole latest objects stream in a ZODB database
package main

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"flag"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/adler32"
	"os"
	"time"

	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/xcommon/log"
	"lab.nexedi.com/kirr/neo/go/xcommon/task"

	"lab.nexedi.com/kirr/neo/go/zodb"
	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"
	"lab.nexedi.com/kirr/neo/go/zodb/storage"

	"github.com/pkg/profile"
)

// hasher is hash.Hash which also knows its name
type hasher struct {
	hash.Hash
	name      string
}

// hasher that discards data
type nullHasher struct {}

func (h nullHasher) Write(b []byte) (int, error)	{ return len(b), nil }
func (h nullHasher) Sum(b []byte) []byte		{ return []byte{0} }
func (h nullHasher) Reset()				{}
func (h nullHasher) Size() int				{ return 1 }
func (h nullHasher) BlockSize() int			{ return 1 }

func main() {
	defer log.Flush()
	fnull    := flag.Bool("null",	false, "don't compute hash - just read data")
	fadler32 := flag.Bool("adler32",false, "compute Adler32 checksum")
	fcrc32   := flag.Bool("crc32",	false, "compute CRC32 checksum")
	fsha1    := flag.Bool("sha1",	false, "compute SHA1 cryptographic hash")
	fsha256  := flag.Bool("sha256", false, "compute SHA256 cryptographic hash")
	fsha512  := flag.Bool("sha512", false, "compute SHA512 cryptographic hash")
	fcheck   := flag.String("check", "",   "verify resulting hash to be = expected")
	fbench   := flag.String("bench", "",   "use benchmarking format for output")
	useprefetch := flag.Bool("useprefetch", false, "prefetch loaded objects")
	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}

	url := flag.Arg(0)

	ctx := context.Background()

	var h hasher
	inith := func(name string, ctor func() hash.Hash) {
		if h.name != "" {
			log.Fatalf(ctx, "duplicate hashes: %s and %s specified", h.name, name)
		}

		h.name = name
		h.Hash = ctor()
	}

	switch {
	case *fnull:	inith("null",	 func() hash.Hash { return nullHasher{} })
	case *fadler32:	inith("adler32", func() hash.Hash { return adler32.New() })
	case *fcrc32:	inith("crc32",   func() hash.Hash { return crc32.NewIEEE() })
	case *fsha1:	inith("sha1",    sha1.New)
	case *fsha256:	inith("sha256",  sha256.New)
	case *fsha512:	inith("sha512",  sha512.New)
	}

	if h.Hash == nil {
		log.Fatal(ctx, "no hash function specified")
	}

	err := zhash(ctx, url, h, *useprefetch, *fbench, *fcheck)
	if err != nil {
		log.Fatal(ctx, err)
	}
}

func zhash(ctx context.Context, url string, h hasher, useprefetch bool, bench, check string) (err error) {
	defer task.Running(&ctx, "zhash")(&err)

	stor, err := zodb.OpenStorageURL(ctx, url)
	if err != nil {
		return err
	}
	defer func() {
		err2 := stor.Close()
		err = xerr.First(err, err2)
	}()

	// XXX always open storage with cache by zodb.OpenStorageURL
	var cache *storage.Cache
	if useprefetch {
		cache = storage.NewCache(stor, 16*1024*1024)
	}

	prefetch := func(ctx context.Context, xid zodb.Xid) {
		if cache != nil {
			//fmt.Printf("prefetch %v\n", xid)
			cache.Prefetch(ctx, xid)
		}
	}

	load := func(ctx context.Context, xid zodb.Xid) (*zodb.Buf, zodb.Tid, error) {
		if cache != nil {
			return cache.Load(ctx, xid)
		} else {
			return stor.Load(ctx, xid)
		}
	}

	const nprefetch = 128	// XXX -> 512 ?

	// prefetchBlk prefetches block of nprefetch objects starting from xid
	//var tprevLoadBlkStart time.Time
	prefetchBlk := func(ctx context.Context, xid zodb.Xid) {
		if cache == nil {
			return
		}

		//t1 := time.Now()
		for i := 0; i < nprefetch; i++ {
			prefetch(ctx, xid)
			xid.Oid++
		}
		//t2 := time.Now()
		//δt := t2.Sub(t1)
		//fmt.Printf("tprefetch: %s", δt)
		//if !tprevLoadBlkStart.IsZero() {
		//	fmt.Printf("\ttprevload: %s", t1.Sub(tprevLoadBlkStart))
		//}
		//fmt.Printf("\n")
		//
		//tprevLoadBlkStart = t2
	}



	lastTid, err := stor.LastTid(ctx)
	if err != nil {
		return err
	}
	before := lastTid + 1	// XXX overflow ?

	if false {
		defer profile.Start(profile.TraceProfile).Stop()
		//defer profile.Start(profile.MemProfile).Stop()
		//defer profile.Start(profile.CPUProfile).Stop()
	}

	tstart := time.Now()

	oid := zodb.Oid(0)
	nread := 0
loop:
	for {
		xid := zodb.Xid{Oid: oid, XTid: zodb.XTid{Tid: before, TidBefore: true}}
		if xid.Oid % nprefetch == 0 {
			prefetchBlk(ctx, xid)
		}
		buf, _, err := load(ctx, xid)
		switch err.(type) {
		case nil:
			// ok
		case *zodb.ErrOidMissing:
			break loop
		default:
			return err
		}

		h.Write(buf.Data)

		//fmt.Fprintf(os.Stderr, "%d @%s\tsha1: %x\n", uint(oid), serial, h.Sum(nil))
		//fmt.Fprintf(os.Stderr, "\tdata: %x\n", buf.Data)

		nread += len(buf.Data)
		oid += 1

		buf.Release()
	}

	tend := time.Now()
	δt := tend.Sub(tstart)

	x := "zhash.go"
	if useprefetch {
		x += fmt.Sprintf("+prefetch%d", nprefetch)
	}
	hresult := fmt.Sprintf("%s:%x", h.name, h.Sum(nil))
	if bench == "" {
		fmt.Printf("%s   ; oid=0..%d  nread=%d  t=%s (%s / object)  x=%s\n",
			hresult, oid-1, nread, δt, δt / time.Duration(oid), x) // XXX /oid cast ?
	} else {
		topic := fmt.Sprintf(bench, x)	// XXX -> better text/template
		fmt.Printf("Benchmark%s 1 %.1f µs/object\t# %s  oid=0..%d  nread=%d  t=%s\n",
			topic, float64(δt) / float64(oid) / float64(time.Microsecond),
			hresult, oid-1, nread, δt)
	}

	if check != "" && hresult != check {
		return fmt.Errorf("%s: hash mismatch: expected %s  ; got %s\t# x=%s", url, check, hresult, x)
	}

	return nil
}
