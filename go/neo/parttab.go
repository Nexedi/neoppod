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
// XXX description:
//
// 	#Np (how-many partitions)    #R (replication factor)
// Cell
// 	.nodeUUID	// .node   (-> .node{UUID,Type,State,Addr})
// 	.cellState
//
//	XXX ? + .haveUpToTid  associated node has data up to such tid
//			= uptodate if haveUpToTid == lastTid
//
//	XXX ? + .plannedToDelete (when after tweak another node will get data
//			  from here and here it will be removed)
//
// 	.backup_tid         # last tid this cell has all data for
// 	.replicating        # currently replicating up to this (.replicating) tid
//
// PT
// 	.id↑
// 	.partition_list [#Np] of []Cell
// 	.count_dict     {} node -> #node_used_in_pt
//
//
// 	 Pt
// 	+-+
// 	| |
// 	+-+  +---------------+ +-----------------+ +-----+
// 	| |  |node,cell_state| |node2,cell_state2| |cell3| ...
// 	+-+  +---------------+ +-----------------+ +-----+
//   Np	| |
// 	+-+
// 	| |
// 	+-+     oid -> PTentry (as PT[oid % Np]
// 	| |     tid
// 	+-+
type PartitionTable struct {
	//ptTab []...
}


// Operational returns whether all object space is covered by at least some ready-to-serve nodes
func (pt *PartitionTable) Operational() bool {
	panic("TODO")
}
