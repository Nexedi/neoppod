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

	"github.com/pkg/profile"
)

func main() {
	defer log.Flush()
	flag.Parse()
	url := flag.Args()[0]	// XXX dirty

	ctx := context.Background()
	err := zsha1(ctx, url)
	if err != nil {
		log.Fatal(ctx, err)
	}
}

func zsha1(ctx context.Context, url string) (err error) {
	defer task.Running(&ctx, "zsha1")(&err)

	stor, err := zodb.OpenStorageURL(ctx, url)
	if err != nil {
		return err
	}
	defer func() {
		err2 := stor.Close()
		err = xerr.First(err, err2)
	}()

	lastTid, err := stor.LastTid(ctx)
	if err != nil {
		return err
	}
	before := lastTid + 1	// XXX overflow ?

	if false {
		defer profile.Start().Stop()
	}

	tstart := time.Now()
	m := sha1.New()

	oid := zodb.Oid(0)
	nread := 0
loop:
	for {
		xid := zodb.Xid{Oid: oid, XTid: zodb.XTid{Tid: before, TidBefore: true}}
		data, _, err := stor.Load(ctx, xid)
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

	fmt.Printf("%x   ; oid=0..%d  nread=%d  t=%s (%s / object)  x=zsha1.go\n",
		m.Sum(nil), oid-1, nread, δt, δt / time.Duration(oid))	// XXX /oid cast ?

	return nil
}
