// Code generated by lab.nexedi.com/kirr/go123/tracing/cmd/gotrace; DO NOT EDIT.

package neo
// code generated for tracepoints

import (
	"lab.nexedi.com/kirr/go123/tracing"
	"unsafe"
)

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
func _trace_exporthash_3520b2da37a17b902760c32971b0fd9ccb6d2ddb() {}
