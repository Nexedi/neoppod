// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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

package neotools
// cli to run master node

import (
	"context"
	"flag"
	"fmt"
	"log"
	"io"
	"os"

	"../../neo/server"
	"../../xcommon/xnet"
)

const masterSummary = "run master node"

// TODO options:
// cluster, masterv ...

func masterUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: neo master [options]
Run NEO master node.
`)

	// FIXME use w (see flags.SetOutput)
}

func masterMain(argv []string) {
	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.Usage = func() { masterUsage(os.Stderr); flags.PrintDefaults() }	// XXX prettify
	cluster := flags.String("cluster", "", "cluster name")
	// XXX masters here too?
	bind := flags.String("bind", "", "address to serve on")
	flags.Parse(argv[1:])

	if *cluster == "" {
		// XXX vvv -> die  or  log.Fatalf ?
		log.Fatal(os.Stderr, "cluster name must be provided")
		os.Exit(2)
	}

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		os.Exit(2)
	}

	net := xnet.NetPlain("tcp")	// TODO + TLS; not only "tcp" ?

	masterSrv := server.NewMaster(*cluster, *bind, net)

	ctx := context.Background()
	/*
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()
	*/

	err := masterSrv.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
