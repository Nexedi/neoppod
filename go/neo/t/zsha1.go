// zsha1 - compute sha1 of whole latest objects stream in a ZODB database
package main

// +build ignore

import (
	"context"
	"crypto/sha1"
	"flag"
	"fmt"
	//"os"
	"time"

	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/xcommon/log"
	"lab.nexedi.com/kirr/neo/go/xcommon/task"

	"lab.nexedi.com/kirr/neo/go/zodb"
	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"
	"lab.nexedi.com/kirr/neo/go/zodb/storage"

	"github.com/pkg/profile"
)

func main() {
	defer log.Flush()
	useprefetch := flag.Bool("useprefetch", false, "prefetch loaded objects")
	flag.Parse()
	url := flag.Args()[0]	// XXX dirty

	ctx := context.Background()
	err := zsha1(ctx, url, *useprefetch)
	if err != nil {
		log.Fatal(ctx, err)
	}
}

func zsha1(ctx context.Context, url string, useprefetch bool) (err error) {
	defer task.Running(&ctx, "zsha1")(&err)

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

	load := func(ctx context.Context, xid zodb.Xid) ([]byte, zodb.Tid, error) {
		if cache != nil {
			return cache.Load(ctx, xid)
		} else {
			return stor.Load(ctx, xid)
		}
	}

	// prefetchBlk prefetches block of 512 objects starting from xid
	//var tprevLoadBlkStart time.Time
	prefetchBlk := func(ctx context.Context, xid zodb.Xid) {
		if cache == nil {
			return
		}

		//t1 := time.Now()
		//for i := 0; i < 512; i++ {
		for i := 0; i < 8; i++ {
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
	}

	tstart := time.Now()
	m := sha1.New()

	oid := zodb.Oid(0)
	nread := 0
loop:
	for {
		xid := zodb.Xid{Oid: oid, XTid: zodb.XTid{Tid: before, TidBefore: true}}
		//if xid.Oid % 512 == 0 {
		if xid.Oid % 8 == 0 {
			prefetchBlk(ctx, xid)
		}
		data, _, err := load(ctx, xid)
		switch err.(type) {
		case nil:
			// ok
		case *zodb.ErrOidMissing:
			break loop
		default:
			return err
		}

		m.Write(data)

		//fmt.Fprintf(os.Stderr, "%d @%s\tsha1: %x\n", uint(oid), serial, m.Sum(nil))
		//fmt.Fprintf(os.Stderr, "\tdata: %x\n", data)

		nread += len(data)
		oid += 1
	}

	tend := time.Now()
	δt := tend.Sub(tstart)

	x := "zsha1.go"
	if useprefetch {
		x += " +prefetch"
	}
	fmt.Printf("%x   ; oid=0..%d  nread=%d  t=%s (%s / object)  x=%s\n",
		m.Sum(nil), oid-1, nread, δt, δt / time.Duration(oid), x) // XXX /oid cast ?

	return nil
}
