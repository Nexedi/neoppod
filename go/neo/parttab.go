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

import "fmt"

// PartitionTable represents object space partitioning in a cluster
//
// It is
//
//	Oid -> []Storage
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

	tab [][]PartitionCell // [#Np] pid -> []Cell

	PTid PTid // ↑ for versioning	XXX -> ver ?	XXX move out of here?
}

// PartitionCell describes one storage in a pid entry in partition table
type PartitionCell struct {
	CellInfo

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

// MakePartTab creates new partition with uniformly distributed nodes
// The partition table created will be of len=np
// FIXME R=1 hardcoded
func MakePartTab(np int, nodev []*Node) *PartitionTable {
	// XXX stub, not tested
	tab := make([][]PartitionCell, np)
	for i, j := 0, 0; i < np; i, j = i+1, j+1 % len(nodev) {
		node := nodev[j]
		// XXX assert node.State > DOWN
		fmt.Printf("tab[%d] <- %v\n", i, node.UUID)
		tab[i] = []PartitionCell{{CellInfo: CellInfo{node.UUID, UP_TO_DATE /*XXX ok?*/}}}
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
// XXX or keep not only NodeUUID in PartitionCell - add *Node ?
func (pt *PartitionTable) OperationalWith(nt *NodeTable) bool {
	for _, ptEntry := range pt.tab {
		if len(ptEntry) == 0 {
			return false
		}

		ok := false
	cellLoop:
		for _, cell := range ptEntry {
			switch cell.CellState {
			case UP_TO_DATE, FEEDING:	// XXX cell.isReadable in py
				// cell says it is readable. let's check whether corresponding node is up
				// FIXME checking whether it is up is not really enough -
				// - what is needed to check is that data on that node is up
				// to last_tid.
				//
				// We leave it as is for now.
				node := nt.Get(cell.NodeUUID)
				if node == nil || node.State != RUNNING {	// XXX PENDING is also ok ?
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

// XXX -> RowList() ?
func (pt *PartitionTable) Dump() []RowInfo { // XXX also include .ptid? -> struct ?
	rowv := make([]RowInfo, len(pt.tab))
	for i, row := range pt.tab {
		cellv := make([]CellInfo, len(row))
		for j, cell := range cellv {
			cellv[j] = CellInfo{NodeUUID: cell.NodeUUID, CellState: cell.CellState}
		}

		rowv[i] = RowInfo{Offset: uint32(i), CellList: cellv}	// XXX cast?
	}
	return rowv
}

func PartTabFromDump(ptid PTid, rowv []RowInfo) *PartitionTable {
	// reconstruct partition table from response
	pt := &PartitionTable{}

	for _, row := range rowv {
		i := row.Offset
		for i >= uint32(len(pt.tab)) {
			pt.tab = append(pt.tab, []PartitionCell{})
		}

		//pt.tab[i] = append(pt.tab[i], row.CellList...)
		for _, cell := range row.CellList {
			pt.tab[i] = append(pt.tab[i], PartitionCell{cell})
		}
	}

	return pt
}
