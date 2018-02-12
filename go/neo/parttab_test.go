// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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

package neo

import (
	"testing"

	"lab.nexedi.com/kirr/neo/go/neo/proto"
)

func TestPartTabOperational(t *testing.T) {
	s1 := proto.UUID(proto.STORAGE, 1)
	s2 := proto.UUID(proto.STORAGE, 2)

	// create nodeinfo for uuid/state
	n := func(uuid proto.NodeUUID, state proto.NodeState) proto.NodeInfo {
		return proto.NodeInfo{UUID: uuid, State: state}	// XXX .Type?
	}

	// create nodetab with [](uuid, state)
	N := func(nodeiv ...proto.NodeInfo) *NodeTable {
		nt := &NodeTable{}
		for _, nodei := range nodeiv {
			nt.Update(nodei)
		}
		return nt
	}

	// create cell with uuid/state
	C := func(uuid proto.NodeUUID, state proto.CellState) Cell {
		return Cell{proto.CellInfo{UUID: uuid, State: state}}
	}

	// shortcut to create []Cell
	v := func(cellv ...Cell) []Cell { return cellv }

	// shortcut to create PartitionTable{[][]Cell}
	P := func(cellvv ...[]Cell) *PartitionTable {
		return &PartitionTable{tab: cellvv}
	}

	var testv = []struct{pt *PartitionTable; nt *NodeTable; operational bool}{
		// empty parttab is non-operational
		{P(), N(), false},
		{P(), N(n(s1, proto.RUNNING)), false},

		// parttab with 1 storage
		{P(v(C(s1, proto.UP_TO_DATE))),	N(), false},
		{P(v(C(s1, proto.UP_TO_DATE))),	N(n(s2, proto.RUNNING)), false},
		{P(v(C(s1, proto.OUT_OF_DATE))),N(n(s1, proto.RUNNING)), false},
		{P(v(C(s1, proto.UP_TO_DATE))),	N(n(s1, proto.RUNNING)), true},
		{P(v(C(s1, proto.FEEDING))),	N(n(s1, proto.RUNNING)), true},

		// TODO more tests
	}

	for _, tt := range testv {
		op := tt.pt.OperationalWith(tt.nt)
		if op != tt.operational {
			t.Errorf("parttab:\n%s\nnodetab:\n%s\noperational: %v  ; want %v\n",
				tt.pt, tt.nt, op, tt.operational)
		}
	}
}
