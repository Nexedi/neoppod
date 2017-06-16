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

package server
// tracepoints

import (
	"../../neo"
	"../../xcommon/tracing"

	_ "unsafe"
)

// TODO autogenerate vvv from "//trace:import path/to/neo"
//      + check consistency (e.g. by hash in neo.trace and here must be the same)

//go:linkname neo_traceConnRecv_Attach _/home/kirr/src/wendelin/neo/neoppod-t/go/neo.traceConnRecv_Attach
func neo_traceConnRecv_Attach(*tracing.Group, func(*neo.Conn, neo.Msg)) *tracing.Probe

//go:linkname neo_traceConnSend_Attach _/home/kirr/src/wendelin/neo/neoppod-t/go/neo.traceConnSend_Attach
func neo_traceConnSend_Attach(*tracing.Group, func(*neo.Conn, neo.Msg)) *tracing.Probe
