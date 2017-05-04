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

package neo
// master node

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

// Master is a node overseeing and managing how whole NEO cluster works
type Master struct {
	custerName   string
	clusterState ClusterState
}

func NewMaster(clusterName string) *Master {
	return &Master{clusterName}
	// XXX .clusterState = RECOVERING ?
}


// ServeLink serves incoming node-node link connection
// XXX +error return?
func (m *Master) ServeLink(ctx context.Context, link *NodeLink) {
	fmt.Printf("master: %s: serving new node\n", link)

	// TODO
}

// ServeClient serves incoming connection on which peer identified itself as client
// XXX +error return?
func (m *Master) ServeClient(ctx context.Context, conn *Conn) {
	// TODO
}

// ServeStorage serves incoming connection on which peer identified itself as storage
// XXX +error return?
func (m *Master) ServeStorage(ctx context.Context, conn *Conn) {
	// TODO

	// >Recovery
	// <AnswerRecovery

	// ? >UnfinishedTransactions
	// ? <AnswerUnfinishedTransactions	(none currently)


	// <NotifyReady
	// >StartOperation



	// >StopOperation (on shutdown)
}

func (m *Master) ServeAdmin(ctx context.Context, conn *Conn) {
	// TODO
}

func (m *Master) ServeMaster(ctx context.Context, conn *Conn) {
	// TODO  (for elections)
}

// ----------------------------------------

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
	var bind string
	var cluster string

	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.Usage = func() { masterUsage(os.Stderr); flags.PrintDefaults() }	// XXX prettify
	flags.StringVar(&bind, "bind", bind, "address to serve on")
	flags.StringVar(&cluster, "cluster", cluster, "cluster name")
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		os.Exit(2)
	}

	masterSrv := NewMaster(cluster)

	ctx := context.Background()
	/*
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()
	*/

	// TODO + TLS
	err := ListenAndServe(ctx, "tcp", bind, masterSrv)	// XXX "tcp" hardcoded
	if err != nil {
		log.Fatal(err)
	}
}
