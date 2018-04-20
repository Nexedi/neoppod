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
// NEO/go event tracer

//go:generate gotrace gen .

import (
	"lab.nexedi.com/kirr/go123/tracing"
	"lab.nexedi.com/kirr/go123/xnet"

	"lab.nexedi.com/kirr/neo/go/neo/neonet"
	"lab.nexedi.com/kirr/neo/go/neo/proto"
)

// TraceCollector connects to NEO-specific trace points via probes and sends events to dispatcher.
//
// XXX naming -> GoTracer	(and PyTracer for NEO/py)
type TraceCollector struct {
	pg *tracing.ProbeGroup
	d  interface { Dispatch(interface{}) }

	node2Name	   map[*NodeApp]string
	nodeTab2Owner	   map[*NodeTable]string
	clusterState2Owner map[*proto.ClusterState]string
}

func NewTraceCollector(dispatch interface { Dispatch(interface{}) }) *TraceCollector {
	return &TraceCollector{
		pg:	&tracing.ProbeGroup{},
		d:	dispatch,

		node2Name:		make(map[*NodeApp]string),
		nodeTab2Owner:		make(map[*NodeTable]string),
		clusterState2Owner:	make(map[*proto.ClusterState]string),
	}
}

//trace:import "lab.nexedi.com/kirr/neo/go/neo/neonet"
//trace:import "lab.nexedi.com/kirr/neo/go/neo/proto"

// Attach attaches the tracer to appropriate trace points.
func (t *TraceCollector) Attach() {
	tracing.Lock()
	//neo_traceMsgRecv_Attach(t.pg, t.traceNeoMsgRecv)
	neonet_traceMsgSendPre_Attach(t.pg, t.traceNeoMsgSendPre)
	proto_traceClusterStateChanged_Attach(t.pg, t.traceClusterState)
	traceNodeChanged_Attach(t.pg, t.traceNode)
	traceMasterStartReady_Attach(t.pg, t.traceMasterStartReady)
	tracing.Unlock()
}

func (t *TraceCollector) Detach() {
	t.pg.Done()
}

// RegisterNode lets the tracer know ptr-to-node-state -> node name relation.
//
// This way it can translate e.g. *NodeTable -> owner node name when creating
// corresponding event.
func (t *TraceCollector) RegisterNode(node *NodeApp, name string) {
	tracing.Lock()
	defer tracing.Unlock()

	// XXX verify there is no duplicate names
	// XXX verify the same pointer is not registerd twice
	t.node2Name[node] = name
	t.nodeTab2Owner[node.NodeTab] = name
	t.clusterState2Owner[&node.ClusterState] = name
}


func (t *TraceCollector) TraceNetConnect(ev *xnet.TraceConnect)	{
	t.d.Dispatch(&eventNetConnect{
		Src:	ev.Src.String(),
		Dst:	ev.Dst.String(),
		Dialed: ev.Dialed,
	})
}

func (t *TraceCollector) TraceNetListen(ev *xnet.TraceListen)	{
	t.d.Dispatch(&eventNetListen{Laddr: ev.Laddr.String()})
}

func (t *TraceCollector) TraceNetTx(ev *xnet.TraceTx)		{} // we use traceNeoMsgSend instead

func (t *TraceCollector) traceNeoMsgSendPre(l *neonet.NodeLink, connID uint32, msg proto.Msg) {
	t.d.Dispatch(&eventNeoSend{l.LocalAddr().String(), l.RemoteAddr().String(), connID, msg})
}

func (t *TraceCollector) traceClusterState(cs *proto.ClusterState) {
	//t.d.Dispatch(&eventClusterState{cs, *cs})
	where := t.clusterState2Owner[cs]
	t.d.Dispatch(&eventClusterState{where, *cs})
}

func (t *TraceCollector) traceNode(nt *NodeTable, n *Node) {
	//t.d.Dispatch(&eventNodeTab{unsafe.Pointer(nt), n.NodeInfo})
	where := t.nodeTab2Owner[nt]
	t.d.Dispatch(&eventNodeTab{where, n.NodeInfo})
}

func (t *TraceCollector) traceMasterStartReady(m *Master, ready bool) {
	//t.d.Dispatch(masterStartReady(m, ready))
	where := t.node2Name[m.node]
	t.d.Dispatch(&eventMStartReady{where, ready})
}

// ----------------------------------------

