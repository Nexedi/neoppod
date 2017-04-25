// Copyright (C) 2016-2017  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Open Source Initiative approved licenses and Convey
// the resulting work. Corresponding source of such a combination shall include
// the source code for all other software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

// neostorage - run a storage node of NEO

package storage

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	//_ "../../storage"	// XXX rel ok?
	neo "../../neo"

	zodb "../../zodb"
	_ "../../zodb/wks"
)


// TODO options:
// cluster, masterv, bind ...

func usage() {
	fmt.Fprintf(os.Stderr,
`neostorage runs one NEO storage server.

Usage: neostorage [options] zstor	XXX
`)
}

// -> StorageMain
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
