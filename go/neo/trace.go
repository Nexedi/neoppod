// Copyright (C) 2016-2017  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Open Source Initiative approved licenses and Convey
// the resulting work. Corresponding source of such a combination shall include
// the source code for all other software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

package neo
// tracepoints

import (
	"unsafe"

	"lab.nexedi.com/kirr/neo/go/xcommon/tracing"
)

// XXX put under 'build +tracedef'

//trace:event
// func traceConnRecv(c *Conn, msg Msg)
// func traceConnSend(c *Conn, msg Msg)	// XXX -> traceConnSendPre ?


// TODO autogenerate vvv from ^^^

// XXX after https://github.com/golang/go/issues/19348 won't cost a function
//     call when tracepoint is disabled

// XXX do we need *_Enabled() ?

var _traceConnRecv []func(*Conn, Msg)
func traceConnRecv(c *Conn, msg Msg) {
	if _traceConnRecv == nil {
		return
	}

	for _, probe := range _traceConnRecv {
		probe(c, msg)
	}
}

// Must be called under tracing.Lock
func traceConnRecv_Attach(probe func(*Conn, Msg)) {
	_traceConnRecv = append(_traceConnRecv, probe)
}


// traceevent: traceConnSend(c *Conn, msg Msg)

type _t_traceConnSend struct {
	tracing.Probe
	probefunc     func(*Conn, Msg)
}

var _traceConnSend *_t_traceConnSend

func traceConnSend(c *Conn, msg Msg) {
	if _traceConnSend != nil {
		_traceConnSend_runprobev(c, msg)
	}
}

func _traceConnSend_runprobev(c *Conn, msg Msg) {
	for p := _traceConnSend; p != nil; p = (*_t_traceConnSend)(unsafe.Pointer(p.Next())) {
		p.probefunc(c, msg)
	}
}

func traceConnSend_Attach(pg *tracing.ProbeGroup, probe func(*Conn, Msg)) *tracing.Probe {
	p := _t_traceConnSend{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_traceConnSend)), &p.Probe)
	return &p.Probe
}
