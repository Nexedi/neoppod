
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

// traceimport TODO lab.nexedi.com/kirr/neo/go/xcommon/xnet/pipenet
