// zsha1 - compute sha1 of whole latest objects stream in a ZODB database
package main

// +build ignore

import (
	"context"
	"crypto/sha1"
	"log"
	"flag"
	"fmt"
	"os"
	"time"

	"lab.nexedi.com/kirr/neo/go/zodb"
	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"

	"github.com/pkg/profile"
)

func main() {
	flag.Parse()
	url := os.Args[1]	// XXX dirty

	bg := context.Background()
	stor, err := zodb.OpenStorageURL(bg, url)
	if err != nil {
		log.Fatal(err)
	}

	lastTid, err := stor.LastTid(bg)
	if err != nil {
		log.Fatal(err)
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
		data, serial, err := stor.Load(bg, xid)
		switch err.(type) {
		case nil:
			// ok
		case *zodb.ErrOidMissing:
			break loop
		default:
			log.Fatal(err)
		}

		m.Write(data)

		fmt.Fprintf(os.Stderr, "%d @%s\tsha1: %x\n", uint(oid), serial, m.Sum(nil))

		nread += len(data)
		oid += 1
	}

	tend := time.Now()

	fmt.Printf("%x   ; oid=0..%d  nread=%d  t=%s\n",
		m.Sum(nil), oid-1, nread, tend.Sub(tstart))
}
