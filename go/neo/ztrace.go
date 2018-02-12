// Code generated by lab.nexedi.com/kirr/go123/tracing/cmd/gotrace; DO NOT EDIT.

package neo
// code generated for tracepoints

import (
	"lab.nexedi.com/kirr/go123/tracing"
	"unsafe"
)

// traceevent: traceMasterStartReady(m *Master, ready bool)

type _t_traceMasterStartReady struct {
	tracing.Probe
	probefunc     func(m *Master, ready bool)
}

var _traceMasterStartReady *_t_traceMasterStartReady

func traceMasterStartReady(m *Master, ready bool) {
	if _traceMasterStartReady != nil {
		_traceMasterStartReady_run(m, ready)
	}
}

func _traceMasterStartReady_run(m *Master, ready bool) {
	for p := _traceMasterStartReady; p != nil; p = (*_t_traceMasterStartReady)(unsafe.Pointer(p.Next())) {
		p.probefunc(m, ready)
	}
}

func traceMasterStartReady_Attach(pg *tracing.ProbeGroup, probe func(m *Master, ready bool)) *tracing.Probe {
	p := _t_traceMasterStartReady{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_traceMasterStartReady)), &p.Probe)
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
func _trace_exporthash_ee76c0bfa710c94614a1fd0fe7a79e9cb723a340() {}
