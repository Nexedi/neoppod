// Copyright (C) 2016-2017  Nexedi SA and Contributors.
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

package neotools
// cli to run storage node

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"lab.nexedi.com/kirr/neo/go/neo/server"
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"
	zt "lab.nexedi.com/kirr/neo/go/zodb/zodbtools"
)

const storageSummary = "run storage node"

func storageUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: neo storage [options] zstor	XXX
Run NEO storage node.
`)

	// FIXME use w (see flags.SetOutput)
}

// TODO set GOMAXPROCS *= N (a lot of file IO) + link
// https://groups.google.com/forum/#!msg/golang-nuts/jPb_h3TvlKE/rQwbg-etCAAJ
// https://github.com/golang/go/issues/6817

func storageMain(argv []string) {
	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.Usage = func() { storageUsage(os.Stderr); flags.PrintDefaults() }	// XXX prettify
	cluster := flags.String("cluster", "", "the cluster name")
	masters := flags.String("masters", "", "list of masters")
	bind := flags.String("bind", "", "address to serve on")
	flags.Parse(argv[1:])

	if *cluster == "" {
		// XXX vvv -> die  or  Fatalf ?
		zt.Fatal(os.Stderr, "cluster name must be provided")
	}

	masterv := strings.Split(*masters, ",")
	if len(masterv) == 0 {
		fmt.Fprintf(os.Stderr, "master list must be provided")
		zt.Exit(2)
	}
	if len(masterv) > 1 {
		fmt.Fprintf(os.Stderr, "BUG neo/go POC currently supports only 1 master")
		zt.Exit(2)
	}

	master := masterv[0]

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		zt.Exit(2)
	}

	// XXX hack to use existing zodb storage for data
	zstor, err := fs1.Open(context.Background(), argv[0])	// XXX context.Background -> ?
	if err != nil {
		zt.Fatal(err)
	}

	net := xnet.NetPlain("tcp")	// TODO + TLS; not only "tcp" ?

	storSrv := server.NewStorage(*cluster, master, *bind, net, zstor)

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
		zt.Fatal(err)
	}
}
