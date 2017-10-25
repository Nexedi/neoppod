// Code generated by lab.nexedi.com/kirr/go123/tracing/cmd/gotrace; DO NOT EDIT.

package server
// code generated for tracepoints

import (
	"lab.nexedi.com/kirr/go123/tracing"
	_ "unsafe"

	"lab.nexedi.com/kirr/neo/go/neo"
)

// traceimport: "lab.nexedi.com/kirr/neo/go/neo"

// rerun "gotrace gen" if you see link failure ↓↓↓
//go:linkname neo_trace_exporthash lab.nexedi.com/kirr/neo/go/neo._trace_exporthash_933f43c04bbb1566c5d1e9ea518f9ed6e0f147a7
func neo_trace_exporthash()
func init() { neo_trace_exporthash() }


//go:linkname neo_traceClusterStateChanged_Attach lab.nexedi.com/kirr/neo/go/neo.traceClusterStateChanged_Attach
func neo_traceClusterStateChanged_Attach(*tracing.ProbeGroup, func(cs *neo.ClusterState)) *tracing.Probe

//go:linkname neo_traceMsgRecv_Attach lab.nexedi.com/kirr/neo/go/neo.traceMsgRecv_Attach
func neo_traceMsgRecv_Attach(*tracing.ProbeGroup, func(c *neo.Conn, msg neo.Msg)) *tracing.Probe

//go:linkname neo_traceMsgSendPre_Attach lab.nexedi.com/kirr/neo/go/neo.traceMsgSendPre_Attach
func neo_traceMsgSendPre_Attach(*tracing.ProbeGroup, func(l *neo.NodeLink, connId uint32, msg neo.Msg)) *tracing.Probe

//go:linkname neo_traceNodeChanged_Attach lab.nexedi.com/kirr/neo/go/neo.traceNodeChanged_Attach
func neo_traceNodeChanged_Attach(*tracing.ProbeGroup, func(nt *neo.NodeTable, n *neo.Node)) *tracing.Probe
