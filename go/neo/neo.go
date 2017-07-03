// Copyright (C) 2016-2017  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

// Package neo and its children provide distributed object storage for ZODB
//
// Package neo itself provides protocol definition and common infrastructure.
// See packages neo.client and neo.server for client and server sides respectively.
// XXX text
package neo

// XXX gotrace ... -> gotrace gen ...
//go:generate sh -c "go run ../xcommon/tracing/cmd/gotrace/gotrace.go ."

import (
	"lab.nexedi.com/kirr/neo/go/zodb"
)

const (
	//INVALID_UUID UUID = 0
	INVALID_TID  zodb.Tid = 1<<64 - 1            // 0xffffffffffffffff
	INVALID_OID  zodb.Oid = 1<<64 - 1

	// OID_LEN = 8
	// TID_LEN = 8
)
