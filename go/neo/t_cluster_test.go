// Copyright (C) 2017-2018  Nexedi SA and Contributors.
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

package neo
// infrastructure for creating NEO test clusters.

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"lab.nexedi.com/kirr/go123/xnet"
	"lab.nexedi.com/kirr/go123/xnet/pipenet"

	"lab.nexedi.com/kirr/neo/go/xcommon/xtracing/tracetest"

	"lab.nexedi.com/kirr/neo/go/neo/storage"
)


// TestCluster ... XXX
type TestCluster struct {
	name	string
	network	*pipenet.Network	// XXX -> lo

	gotracer	*TraceCollector		// XXX -> GoTracer
	//tpy	*PyTracer

	erouter		*EventRouter
	edispatch	*tracetest.EventDispatcher

	tabMu     sync.Mutex
	nodeTab	  map[string/*node*/]*tNode
	checkTab  map[string/*node*/]*tracetest.EventChecker

	ttest	testing.TB	// original testing env this cluster was created at
}

// tNode represents information about a test node ... XXX
type tNode struct {
	net xnet.Networker
}

// XXX stub
type ITestMaster interface {}
type ITestStorage interface {}
type ITestClient interface {}

// NewTestCluster creates new NEO test cluster.
//
// XXX ...
//
// XXX defer t.Stop()
func NewTestCluster(ttest testing.TB, name string) *TestCluster {
	t := &TestCluster{
		name:	name,
		network: pipenet.New("testnet"),	// test network

		nodeTab:  make(map[string]*tNode),
		checkTab: make(map[string]*tracetest.EventChecker),

		//...			XXX
		ttest:	ttest,
	}

	t.erouter	= NewEventRouter()
	t.edispatch	= tracetest.NewEventDispatcher(t.erouter)
	t.gotracer	= NewTraceCollector(t.edispatch)

	t.gotracer.Attach()
	return t
}

// Stop stops the cluster.
//
// All processes of the cluster are stopped ...	XXX
// XXX do we need error return?
func (t *TestCluster) Stop() error {
	//...	XXX
	t.gotracer.Detach()
	//XXX t.pytracer.Detach()


	return nil
}

// Checker returns tracechecker corresponding to name.
//
// name might be of "node" or "node1-node2" kind.	XXX more text
// node or node1/node2 must be already registered.
func (t *TestCluster) Checker(name string) *tracetest.EventChecker {
	t.tabMu.Lock()
	defer t.tabMu.Unlock()

	c, ok := t.checkTab[name]
	if !ok {
		panic(fmt.Sprintf("test cluster: no %q checker", name))
	}

	return c
}

// registerNewNode registers new node with given name.
//
// the node is registered in .nodeTab and .checkTab is populated ... XXX
//
// the node must not be registered before.
func (t *TestCluster) registerNewNode(name string) *tNode {
	t.tabMu.Lock()
	defer t.tabMu.Unlock()

	// check node not yet registered
	if _, already := t.nodeTab[name]; already {
		panic(fmt.Sprintf("test cluster: node %q registered twice", name))
	}

	// tracechecker for events on node
	c1 := tracetest.NewSyncChan(name) // trace of events local to node
	t.erouter.BranchNode(name, c1)
	t.checkTab[name] = tracetest.NewEventChecker(t.ttest, t.edispatch, c1)

	// tracecheckers for events on links of all node1-node2 pairs
	for name2 := range t.nodeTab {
		// trace of events with cause root being node1 -> node2 send
		c12 := tracetest.NewSyncChan(name + "-" + name2)
		// ----//---- node2 -> node1 send
		c21 := tracetest.NewSyncChan(name2 + "-" + name)

		t12 := tracetest.NewEventChecker(t.ttest, t.edispatch, c12)
		t21 := tracetest.NewEventChecker(t.ttest, t.edispatch, c21)

		t.erouter.BranchLink(name + "-" + name2, c12, c21)
		t.checkTab[name + "-" + name2] = t12
		t.checkTab[name2 + "-" + name] = t21
	}

	node := &tNode{}
	node.net  = xnet.NetTrace(t.network.Host(name), t.gotracer)

	t.nodeTab[name] = node
	return node
}


// NewMaster creates new master on node name.
//
// The master will be accepting incoming connections at node:1.
// The node must be not yet existing and will be dedicated to the created master fully.	XXX
//
// XXX error of creating py process?
func (t *TestCluster) NewMaster(name string) ITestMaster {
	node := t.registerNewNode(name)
	return tNewMaster(t.name, ":1", node.net)
}

func (t *TestCluster) NewStorage(name, masterAddr string, back storage.Backend) ITestStorage {
	node := t.registerNewNode(name)
	return tNewStorage(t.name, masterAddr, ":1", node.net, back)
}

func (t *TestCluster) NewClient(name, masterAddr string) ITestClient {
	node := t.registerNewNode(name)
	return newClient(t.name, masterAddr, node.net)
}



// test-wrapper around Storage - to automatically listen by address, not provided listener.
type tStorage struct {
	*Storage
	serveAddr string
}

func tNewStorage(clusterName, masterAddr, serveAddr string, net xnet.Networker, back storage.Backend) *tStorage {
	return &tStorage{
		Storage:   NewStorage(clusterName, masterAddr, net, back),
		serveAddr: serveAddr,
	}
}

func (s *tStorage) Run(ctx context.Context) error {
	l, err := s.node.Net.Listen(s.serveAddr)
	if err != nil {
		return err
	}

	return s.Storage.Run(ctx, l)
}

// test-wrapper around Master
//
// - automatically listens by address, not provided listener.
// - uses virtual clock.
type tMaster struct {
	*Master
	serveAddr string
	vclock    vclock
}

func tNewMaster(clusterName, serveAddr string, net xnet.Networker) *tMaster {
	m := &tMaster{
		Master:   NewMaster(clusterName, net),
		serveAddr: serveAddr,
	}
	m.Master.monotime = m.vclock.monotime
	return m
}

func (m *tMaster) Run(ctx context.Context) error {
	l, err := m.node.Net.Listen(m.serveAddr)
	if err != nil {
		return err
	}

	return m.Master.Run(ctx, l)
}
