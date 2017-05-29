// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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
// partition table

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
//	ptid(oid) = oid % Np		XXX ptid -> pid ?
//
// rule. The `oid % Np` is known as partition identifier of oid.
//
// There is also externally set "redundancy factor" R which describes the minimum
// amount of separate nodes a particular object data should be stored into at the same time.
//
// Given Np, R and []Storage PartitionTable tries to organize
//
//	Ptid -> []Storage
//
// mapping so that
//
// - redundancy level set by R is met
// - storages associated with adjacent Ptids are different
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
// Np thus is always multiple of Ns and with furter reorderings (if needed)
// could be reduced directly to Ns.
//
// Usually Master maintains partition table, plans partition updates and tells
// storages to executed them, and broadcasts partition table updates to all
// nodes in the cluster.
//
// PartitionTable zero value is valid empty partition table.
type PartitionTable struct {
	// XXX do we need sync.Mutex here for updates ?

	ptTab [][]PartitionCell // [#Np]

	ptId int // ↑ for versioning	XXX -> ver ?
}

// PartitionCell describes one storage in a ptid entry in partition table
type PartitionCell struct {
	NodeUUID
	CellState

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


// Operational returns whether all object space is covered by at least some ready-to-serve nodes
// NOTE XXX operational here means only pt itself is operational
//          for cluster to be really operational it has to be checked whether
//          nodes referenced by pt are up and running
//
// XXX or keep not only NodeUUID in PartitionCell - add *Node ?
//
// XXX -> add `nt *NodeTable` as argument and check real node states there ?
func (pt *PartitionTable) Operational() bool {
	for _, ptEntry := range pt.ptTab {
		if len(ptEntry) == 0 {
			return false
		}

		ok := false
	cellLoop:
		for _, cell := range ptEntry {
			switch cell.CellState {
			case UP_TO_DATE, FEEDING:	// XXX cell.isReadble in py
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
