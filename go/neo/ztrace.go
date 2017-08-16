// Code generated by lab.nexedi.com/kirr/go123/tracing/cmd/gotrace; DO NOT EDIT.

package neo
// code generated for tracepoints

import (
	"lab.nexedi.com/kirr/neo/go/xcommon/tracing"
	"unsafe"
)

// traceevent: traceClusterStateChanged(cs *ClusterState)

type _t_traceClusterStateChanged struct {
	tracing.Probe
	probefunc     func(cs *ClusterState)
}

var _traceClusterStateChanged *_t_traceClusterStateChanged

func traceClusterStateChanged(cs *ClusterState) {
	if _traceClusterStateChanged != nil {
		_traceClusterStateChanged_run(cs)
	}
}

func _traceClusterStateChanged_run(cs *ClusterState) {
	for p := _traceClusterStateChanged; p != nil; p = (*_t_traceClusterStateChanged)(unsafe.Pointer(p.Next())) {
		p.probefunc(cs)
	}
}

func traceClusterStateChanged_Attach(pg *tracing.ProbeGroup, probe func(cs *ClusterState)) *tracing.Probe {
	p := _t_traceClusterStateChanged{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_traceClusterStateChanged)), &p.Probe)
	return &p.Probe
}

// traceevent: traceConnRecv(c *Conn, msg Msg)

type _t_traceConnRecv struct {
	tracing.Probe
	probefunc     func(c *Conn, msg Msg)
}

var _traceConnRecv *_t_traceConnRecv

func traceConnRecv(c *Conn, msg Msg) {
	if _traceConnRecv != nil {
		_traceConnRecv_run(c, msg)
	}
}

func _traceConnRecv_run(c *Conn, msg Msg) {
	for p := _traceConnRecv; p != nil; p = (*_t_traceConnRecv)(unsafe.Pointer(p.Next())) {
		p.probefunc(c, msg)
	}
}

func traceConnRecv_Attach(pg *tracing.ProbeGroup, probe func(c *Conn, msg Msg)) *tracing.Probe {
	p := _t_traceConnRecv{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_traceConnRecv)), &p.Probe)
	return &p.Probe
}

// traceevent: traceConnSendPre(c *Conn, msg Msg)

type _t_traceConnSendPre struct {
	tracing.Probe
	probefunc     func(c *Conn, msg Msg)
}

var _traceConnSendPre *_t_traceConnSendPre

func traceConnSendPre(c *Conn, msg Msg) {
	if _traceConnSendPre != nil {
		_traceConnSendPre_run(c, msg)
	}
}

func _traceConnSendPre_run(c *Conn, msg Msg) {
	for p := _traceConnSendPre; p != nil; p = (*_t_traceConnSendPre)(unsafe.Pointer(p.Next())) {
		p.probefunc(c, msg)
	}
}

func traceConnSendPre_Attach(pg *tracing.ProbeGroup, probe func(c *Conn, msg Msg)) *tracing.Probe {
	p := _t_traceConnSendPre{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_traceConnSendPre)), &p.Probe)
	return &p.Probe
}

// traceevent: traceNodeChanged(nt *NodeTable, n *Node)

type _t_traceNodeChanged struct {
	tracing.Probe
	probefunc     func(nt *NodeTable, n *Node)
}

var _traceNodeChanged *_t_traceNodeChanged

func traceNodeChanged(nt *NodeTable, n *Node) {
	if _traceNodeChanged != nil {
		_traceNodeChanged_run(nt, n)
	}
}

func _traceNodeChanged_run(nt *NodeTable, n *Node) {
	for p := _traceNodeChanged; p != nil; p = (*_t_traceNodeChanged)(unsafe.Pointer(p.Next())) {
		p.probefunc(nt, n)
	}
}

func traceNodeChanged_Attach(pg *tracing.ProbeGroup, probe func(nt *NodeTable, n *Node)) *tracing.Probe {
	p := _t_traceNodeChanged{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_traceNodeChanged)), &p.Probe)
	return &p.Probe
}

// trace export signature
func _trace_exporthash_ab325b43be064a06d1c80db96d5bf50678b5b037() {}
