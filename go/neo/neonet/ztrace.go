// Code generated by lab.nexedi.com/kirr/go123/tracing/cmd/gotrace; DO NOT EDIT.

package neonet
// code generated for tracepoints

import (
	"lab.nexedi.com/kirr/go123/tracing"
	"unsafe"

	"lab.nexedi.com/kirr/neo/go/neo/proto"
)

// traceevent: traceMsgRecv(c *Conn, msg proto.Msg)

type _t_traceMsgRecv struct {
	tracing.Probe
	probefunc     func(c *Conn, msg proto.Msg)
}

var _traceMsgRecv *_t_traceMsgRecv

func traceMsgRecv(c *Conn, msg proto.Msg) {
	if _traceMsgRecv != nil {
		_traceMsgRecv_run(c, msg)
	}
}

func _traceMsgRecv_run(c *Conn, msg proto.Msg) {
	for p := _traceMsgRecv; p != nil; p = (*_t_traceMsgRecv)(unsafe.Pointer(p.Next())) {
		p.probefunc(c, msg)
	}
}

func traceMsgRecv_Attach(pg *tracing.ProbeGroup, probe func(c *Conn, msg proto.Msg)) *tracing.Probe {
	p := _t_traceMsgRecv{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_traceMsgRecv)), &p.Probe)
	return &p.Probe
}

// traceevent: traceMsgSendPre(l *NodeLink, connId uint32, msg proto.Msg)

type _t_traceMsgSendPre struct {
	tracing.Probe
	probefunc     func(l *NodeLink, connId uint32, msg proto.Msg)
}

var _traceMsgSendPre *_t_traceMsgSendPre

func traceMsgSendPre(l *NodeLink, connId uint32, msg proto.Msg) {
	if _traceMsgSendPre != nil {
		_traceMsgSendPre_run(l, connId, msg)
	}
}

func _traceMsgSendPre_run(l *NodeLink, connId uint32, msg proto.Msg) {
	for p := _traceMsgSendPre; p != nil; p = (*_t_traceMsgSendPre)(unsafe.Pointer(p.Next())) {
		p.probefunc(l, connId, msg)
	}
}

func traceMsgSendPre_Attach(pg *tracing.ProbeGroup, probe func(l *NodeLink, connId uint32, msg proto.Msg)) *tracing.Probe {
	p := _t_traceMsgSendPre{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_traceMsgSendPre)), &p.Probe)
	return &p.Probe
}

// trace export signature
func _trace_exporthash_c54acca8f21ba38c3ba9672c3d38021c3c8b9484() {}