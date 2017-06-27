
// traceevent: traceConnRecv(c *Conn, msg Msg)	XXX better raw .Text (e.g. comments)

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

func _traceConnRecv_run(c, msg) {
	for p := _traceConnRecv; p != nil; p = (*_t_traceConnRecv)(unsafe.Pointer(p.Next())) {
		p.probefunc(c, msg)
	}
}

func traceConnRecv_Attach(pg *tracing.ProbeGroup, probe func(c *Conn, msg Msg)) *tracing.Probe {
	p := _t_traceConnRecv{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_traceConnRecv), &p.Probe)
	return &p.Probe
}

// traceevent: traceConnSend(c *Conn, msg Msg)	XXX better raw .Text (e.g. comments)

type _t_traceConnSend struct {
	tracing.Probe
	probefunc     func(c *Conn, msg Msg)
}

var _traceConnSend *_t_traceConnSend

func traceConnSend(c *Conn, msg Msg) {
	if _traceConnSend != nil {
		_traceConnSend_run(c, msg)
	}
}

func _traceConnSend_run(c, msg) {
	for p := _traceConnSend; p != nil; p = (*_t_traceConnSend)(unsafe.Pointer(p.Next())) {
		p.probefunc(c, msg)
	}
}

func traceConnSend_Attach(pg *tracing.ProbeGroup, probe func(c *Conn, msg Msg)) *tracing.Probe {
	p := _t_traceConnSend{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_traceConnSend), &p.Probe)
	return &p.Probe
}

// traceimport: lab.nexedi.com/kirr/neo/go/xcommon/xnet/pipenet

//go:linkname pipenet_traceAccept_Attach lab.nexedi.com/kirr/neo/go/xcommon/xnet/pipenet.traceAccept_Attach
func pipenet_traceAccept_Attach(*tracing.ProbeGroup, func(conn net.Conn)) *tracing.Probe

//go:linkname pipenet_traceDial_Attach lab.nexedi.com/kirr/neo/go/xcommon/xnet/pipenet.traceDial_Attach
func pipenet_traceDial_Attach(*tracing.ProbeGroup, func(addr string)) *tracing.Probe

//go:linkname pipenet_traceListen_Attach lab.nexedi.com/kirr/neo/go/xcommon/xnet/pipenet.traceListen_Attach
func pipenet_traceListen_Attach(*tracing.ProbeGroup, func(laddr string)) *tracing.Probe

//go:linkname pipenet_traceNew_Attach lab.nexedi.com/kirr/neo/go/xcommon/xnet/pipenet.traceNew_Attach
func pipenet_traceNew_Attach(*tracing.ProbeGroup, func(name string)) *tracing.Probe

//go:linkname pipenet_traceNewHost_Attach lab.nexedi.com/kirr/neo/go/xcommon/xnet/pipenet.traceNewHost_Attach
func pipenet_traceNewHost_Attach(*tracing.ProbeGroup, func(host *Host)) *tracing.Probe
