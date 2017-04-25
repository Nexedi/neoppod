// TODO copyright / license

// neostorage - run a storage node of NEO

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	//_ "../../storage"	// XXX rel ok?
	neo "../.."

	zodb "../../../zodb"
	_ "../../../zodb/wks"
)


// TODO options:
// cluster, masterv, bind ...

func usage() {
	fmt.Fprintf(os.Stderr,
`neostorage runs one NEO storage server.

Usage: neostorage [options] zstor	XXX
`)
}

func main() {
	flag.Usage = usage
	flag.Parse()
	argv := flag.Args()

	if len(argv) == 0 {
		usage()
		os.Exit(2)
	}

	zstor, err := zodb.OpenStorageURL(argv[0])
	if err != nil {
		log.Fatal(err)
	}

	storsrv := neo.NewStorage(zstor)

	/*
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Second)
		cancel()
	}()
	*/
	ctx := context.Background()

	err = neo.ListenAndServe(ctx, "tcp", "localhost:1234", storsrv)
	if err != nil {
		log.Fatal(err)
	}
}
