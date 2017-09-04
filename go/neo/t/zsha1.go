// zsha1 - compute sha1 of whole latest objects stream in a ZODB database
package main

import (
	"context"
	"crypto/sha1"
	"log"
	"fmt"
	"os"
	"time"

	"lab.nexedi.com/kirr/neo/go/zodb"
	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"

	"github.com/pkg/profile"
)

func main() {
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

	defer profile.Start().Stop()

	tstart := time.Now()
	m := sha1.New()

	oid := zodb.Oid(0)
	nread := 0
loop:
	for {
		xid := zodb.Xid{Oid: oid, XTid: zodb.XTid{Tid: before, TidBefore: true}}
		data, _, err := stor.Load(bg, xid)
		switch err.(type) {
		case nil:
			// ok
		case *zodb.ErrOidMissing:
			break loop
		default:
			log.Fatal(err)
		}

		m.Write(data)

		nread += len(data)
		oid += 1
	}

	tend := time.Now()

	fmt.Printf("%x   ; oid=0..%d  nread=%d  t=%s\n",
		m.Sum(nil), oid-1, nread, tend.Sub(tstart))
}
