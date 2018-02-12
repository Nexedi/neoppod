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
// partition table

import (
	"bytes"
	"fmt"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/neo/proto"
)

// PartitionTable represents object space partitioning in a cluster
//
// It is
//
//	Oid -> []Storage	# XXX actually oid -> []uuid
//
// mapping associating object id with list of storage nodes on where data for
// this oid should be written-to/loaded-from. This mapping is organized as follows:
//
// Oid space is divided (partitioned) into Np parts via
//
//	pid(oid) = oid % Np
//
// rule. The `oid % Np` is known as partition identifier of oid.
//
// There is also externally set "redundancy factor" R which describes the minimum
// amount of separate nodes a particular object data should be stored into at the same time.
//
// Given Np, R and []Storage PartitionTable tries to organize
//
//	pid -> []Storage
//
// mapping so that
//
// - redundancy level set by R is met
// - storages associated with adjacent pids are different
//
// when such organization is reached the partition table is called operational
// and non-operational otherwise.	XXX and if storages are ready
//
// The PartitionTable can auto-adjust to storage nodes coming and going.
// Let's consider cluster change from Ns1 to Ns2 storage nodes: if we pick
//
//	Np = LCM(Ns1, Ns2)
//
// XXX ^^^ and vvv R is not taken into account.
//
// then it is possible to rebalance data with minimal amount of data movement
//
//	~ min(Ns1, Ns2)·size(partition)
//
// Examples:
//
// [S1, S2] + S3:		LCM(2,3) = 6
//
//	S1     S1     S1
//	S2  ~  S2     S2
//	       S1  →  S3
//	       S2     S2
//	       S1     S1
//	       S2  →  S3
//
//
// [S1, S2, S3] + S4:		LCM(3,4) = 12
//
//	S1     S1     S1
//	S2     S2     S2
//	S3     S3     S3
//	S2     S2  →  S4
//	S1     S1     S1
//	S3  ~  S3     S3
//	       S1  →  S4
//	       S2     S2
//	       S3     S3
//	       S2     S2
//	       S1     S1
//	       S3  →  S4
//
//
// [S1, S2, S3] - S3:		LCM(3,2) = 6
//
//	S1     S1     S1
//	S2     S2     S2
//	S3  ~  S3  →  S1
//	       S1     S1
//	       S2     S2
//	       S3  →  S2
//
// Np thus is always multiple of Ns and with further reorderings (if needed)
// could be reduced directly to Ns.
//
// Usually Master maintains partition table, plans partition updates and tells
// storages to executed them, and broadcasts partition table updates to all
// nodes in the cluster.
//
// PartitionTable zero value is valid empty partition table.
type PartitionTable struct {
	// XXX do we need sync.Mutex here for updates ?

	tab [][]Cell // [#Np] pid -> []Cell

	PTid proto.PTid // ↑ for versioning	XXX -> ver ?	XXX move out of here?
}

// Cell describes one storage in a pid entry in partition table
type Cell struct {
	proto.CellInfo // .uuid + .state

//	XXX ? + .haveUpToTid  associated node has data up to such tid
//			= uptodate if haveUpToTid == lastTid
//
//	XXX ? + .plannedToDelete (when after tweak another node will get data
//			  from here and here it will be removed)
//
//	XXX something describing in-progress updates vvv ?
// 	.backup_tid         # last tid this cell has all data for
// 	.replicating        # currently replicating up to this (.replicating) tid
//
}

// Get returns cells oid is associated with.
func (pt *PartitionTable) Get(oid zodb.Oid) []Cell {
	if len(pt.tab) == 0 {
		return nil
	}
	pid := uint64(oid) % uint64(len(pt.tab))
	return pt.tab[pid]
}

// Readable reports whether it is ok to read data from a cell
func (c *Cell) Readable() bool {
	switch c.State {
	case proto.UP_TO_DATE, proto.FEEDING:
		return true
	}
	return false
}

// MakePartTab creates new partition with uniformly distributed nodes
// The partition table created will be of len=np
// FIXME R=1 hardcoded
func MakePartTab(np int, nodev []*Node) *PartitionTable {
	// XXX stub, not tested
	tab := make([][]Cell, np)
	for i, j := 0, 0; i < np; i, j = i+1, j+1 % len(nodev) {
		node := nodev[j]
		// XXX assert node.State > DOWN
		//fmt.Printf("tab[%d] <- %v\n", i, node.UUID)
		tab[i] = []Cell{{CellInfo: proto.CellInfo{node.UUID, proto.UP_TO_DATE /*XXX ok?*/}}}
	}

	return &PartitionTable{tab: tab}
}


// OperationalWith checks whether all object space is covered by at least some ready-to-serve nodes
//
// for all partitions it checks both:
// - whether there are up-to-date entries in the partition table, and
// - whether there are corresponding storage nodes that are up
//
// information about nodes being up or down is obtained from supplied NodeTable
//
// XXX or keep not only NodeUUID in Cell - add *Node ?
func (pt *PartitionTable) OperationalWith(nt *NodeTable) bool {
	// empty partition table is never operational
	if len(pt.tab) == 0 {
		return false
	}

	for _, ptEntry := range pt.tab {
		if len(ptEntry) == 0 {
			return false
		}

		ok := false
	cellLoop:
		for _, cell := range ptEntry {
			if cell.Readable() {
				// cell says it is readable. let's check whether corresponding node is up
				// FIXME checking whether it is up is not really enough -
				// - what is needed to check is that data on that node is up
				// to last_tid.
				//
				// We leave it as is for now.
				node := nt.Get(cell.UUID)
				if node == nil || node.State != proto.RUNNING {	// XXX PENDING is also ok ?
					continue
				}

				ok = true
				break cellLoop
			}
		}
		if !ok {
			return false
		}

	}

	return true
}


// ---- encode / decode PT to / from messages
// XXX naming

func (pt *PartitionTable) String() string {
	buf := &bytes.Buffer{}

	fmt.Fprintf(buf, "PT.%d[%d]\n", pt.PTid, len(pt.tab))
	for i, cellv := range pt.tab {
		fmt.Fprintf(buf, "%d:\t", i)
		if len(cellv) == 0 {
			fmt.Fprintf(buf, "ø\n")
			continue
		}

		for _, cell := range cellv {
			fmt.Fprintf(buf, " %s(%s)", cell.UUID, cell.State)
		}
		fmt.Fprintf(buf, "\n")
	}

	return buf.String()
}

// XXX -> RowList() ?
func (pt *PartitionTable) Dump() []proto.RowInfo { // XXX also include .ptid? -> struct ?
	rowv := make([]proto.RowInfo, len(pt.tab))
	for i, row := range pt.tab {
		cellv := make([]proto.CellInfo, len(row))
		for j, cell := range row {
			cellv[j] = cell.CellInfo
		}

		rowv[i] = proto.RowInfo{Offset: uint32(i), CellList: cellv}	// XXX cast?
	}
	return rowv
}

func PartTabFromDump(ptid proto.PTid, rowv []proto.RowInfo) *PartitionTable {
	// reconstruct partition table from response
	pt := &PartitionTable{}
	pt.PTid = ptid

	for _, row := range rowv {
		i := row.Offset
		for i >= uint32(len(pt.tab)) {
			pt.tab = append(pt.tab, []Cell{})
		}

		//pt.tab[i] = append(pt.tab[i], row.CellList...)
		for _, cell := range row.CellList {
			pt.tab[i] = append(pt.tab[i], Cell{cell})
		}
	}

	return pt
}
