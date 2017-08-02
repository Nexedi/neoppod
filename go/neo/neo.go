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

// Package neo and its children provide distributed object storage for ZODB.
//
// Package neo itself provides protocol definition and common infrastructure.
// See packages neo.client and neo.server for client and server sides respectively.
// XXX text
package neo

// XXX gotrace ... -> gotrace gen ...
//go:generate sh -c "go run ../xcommon/tracing/cmd/gotrace/{gotrace,util}.go ."

import (
	"lab.nexedi.com/kirr/neo/go/xcommon/xnet"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

const (
	//INVALID_UUID UUID = 0
	INVALID_TID  zodb.Tid = 1<<64 - 1            // 0xffffffffffffffff
	INVALID_OID  zodb.Oid = 1<<64 - 1

	// OID_LEN = 8
	// TID_LEN = 8
)


// NodeCommon is common data in all NEO nodes: Master, Storage & Client	XXX text
// XXX naming -> Node ?
type NodeCommon struct {
	MyInfo		NodeInfo	// XXX -> only NodeUUID
	ClusterName	string

	Net		xnet.Networker	// network AP we are sending/receiving on
	MasterAddr	string		// address of master	XXX -> Address ?

	// XXX + NodeTab (information about nodes in the cluster) ?
	// XXX + PartTab (information about data distribution in the cluster) ?
}

// Listen starts listening at node's listening address.
// If the address is empty one new free is automatically selected.
// The node information about where it listens at is appropriately updated.
func (n *NodeCommon) Listen() (*Listener, error) {
	// start listening
	l, err := Listen(n.Net, n.MyInfo.Address.String())	// XXX ugly
	if err != nil {
		return nil, err	// XXX err ctx
	}

	// now we know our listening address (in case it was autobind before)
	// NOTE listen("tcp", ":1234") gives l.Addr 0.0.0.0:1234 and
	//      listen("tcp6", ":1234") gives l.Addr [::]:1234
	//	-> host is never empty
	addr, err := Addr(l.Addr())
	if err != nil {
		// XXX -> panic here ?
		l.Close()
		return nil, err	// XXX err ctx
	}

	n.MyInfo.Address = addr

	return l, nil
}

// XXX func (n *Node) IdentifyWith(...) ?
//	XXX better -> Connect() (=Dial, IdentifyWith, process common ID reply ...)

// TODO functions to update:
//	.PartTab	from NotifyPartitionTable msg
//	.NodeTab	from NotifyNodeInformation msg
//	.ClusterState	from NotifyClusterState msg
