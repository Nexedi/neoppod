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

// XXX put under 'build +tracedef'
// func traceConnRecv(c *Conn, msg Msg)
// func traceConnSend(c *Conn, msg Msg)	// XXX -> traceConnSendPre ?


// TODO autogenerate vvv from ^^^

// XXX after https://github.com/golang/go/issues/19348 won't cost a function
//     call when tracepoint is disabled

// XXX do we need *_Enabled() ?

var _traceConnRecv func(*Conn, Msg)
func traceConnRecv(c *Conn, msg Msg) {
	println("A traceConnRecv", &_traceConnRecv)
	if _traceConnRecv != nil {
	println("  _traceConnRecv")
		_traceConnRecv(c, msg)
	}
}

var _traceConnSend func(*Conn, Msg)
func traceConnSend(c *Conn, msg Msg) {
	println("A traceConnSend", &_traceConnSend)
	if _traceConnSend != nil {
	println("  _traceConnSend")
		_traceConnSend(c, msg)
	}
}
