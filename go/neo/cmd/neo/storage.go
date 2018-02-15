// Copyright (C) 2016-2018  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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

package main
// cli to run storage node

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"lab.nexedi.com/kirr/go123/prog"
	"lab.nexedi.com/kirr/go123/xnet"
	"lab.nexedi.com/kirr/neo/go/neo"
	"lab.nexedi.com/kirr/neo/go/neo/storage"

	_ "lab.nexedi.com/kirr/neo/go/neo/storage/fs1"
	_ "lab.nexedi.com/kirr/neo/go/neo/storage/sqlite"
)

const storageSummary = "run storage node"

func storageUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: neo storage [options] <data>
Run NEO storage node.

<data> is backend URL for data persistence.

Available backends are:

`)
	for _, back := range storage.AvailableBackends() {
		fmt.Fprintf(w, "- %s://\n", back)
	}

	fmt.Fprintf(w,
`
XXX currently storage is read-only.

`)
}

func storageMain(argv []string) {
	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.Usage = func() { storageUsage(os.Stderr); flags.PrintDefaults() }	// XXX prettify
	cluster := flags.String("cluster", "", "the cluster name")
	masters := flags.String("masters", "", "list of masters")
	bind := flags.String("bind", "", "address to serve on")
	flags.Parse(argv[1:])

	if *cluster == "" {
		prog.Fatal("cluster name must be provided")
	}

	masterv := strings.Split(*masters, ",")
	if len(masterv) == 0 {
		prog.Fatal("master list must be provided")
	}
	if len(masterv) > 1 {
		prog.Fatal("BUG neo/go POC currently supports only 1 master")
	}

	master := masterv[0]

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		prog.Exit(2)
	}

	// adjust GOMAXPROCS *= N (a lot of file IO) because file IO really consumes OS threads; details:
	// https://groups.google.com/forum/#!msg/golang-nuts/jPb_h3TvlKE/rQwbg-etCAAJ
	// https://github.com/golang/go/issues/6817
	//
	// XXX check how varying this affects performance
	maxprocs := runtime.GOMAXPROCS(0)
	runtime.GOMAXPROCS(maxprocs*8)		// XXX *8 is enough?


	back, err := storage.OpenBackend(context.Background(), argv[0])
	if err != nil {
		prog.Fatal(err)
	}

	net := xnet.NetPlain("tcp")	// TODO + TLS; not only "tcp" ?

	storSrv := neo.NewStorage(*cluster, master, *bind, net, back)

	ctx := context.Background()
	/*
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()
	*/

	err = storSrv.Run(ctx)
	if err != nil {
		prog.Fatal(err)
	}
}
