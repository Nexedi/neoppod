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
	"testing"

	"lab.nexedi.com/kirr/go123/xnet/pipenet"
)


// TestCluster ... XXX
type TestCluster struct {
	name	string
	net	*pipenet.Network	// XXX -> lo

	gotracer	*TraceCollector		// XXX -> GoTracer
	//tpy	*PyTracer

	ttest	testing.TB	// original testing env this cluster was created at
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
	return &TestCluster{
		name:	name,
		//...			XXX
		ttest:	ttest,
	}
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

// NewMaster creates new master on node.
//
// The master will be accepting incoming connections at node:1.
// The node must be not yet existing and will be dedicated to the created master fully.	XXX
//
// XXX error of creating py process?
func (t *TestCluster) NewMaster(node string) ITestMaster {
	//...	XXX

	// XXX check name is unique host name - not already registered
	// XXX set M clock to vclock.monotime

	// tracetest.NewSyncChan("m.main")
	// foreach node1,node2:
	//	tracetest.NewChan("node1-node2")  // trace of events with cause root being n1 -> n2 send
	//	tracetest.NewChan("node2-node1")  // trace of events with cause root being n2 -> n1 send

	// for each created tracetest.Chan -> create tracetest.EventChecker

	//rt.BranchNode("m", cM)
	//rt.BranchState("m", 
	//rt.BranchLink("n1-n2", ..., ...)

	// XXX state on S,C is controlled by M:
	// rt.BranchState("s", cMS)

	return nil
}

func (t *TestCluster) NewStorage(node string) ITestStorage {
	panic("TODO")
}

func (t *TestCluster) NewClient(node string) ITestClient {
	panic("TODO")
}
