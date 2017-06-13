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

// XXX goes away - we don't need it.

package neo
// cluster state	XXX

// ClusterInfo represents information about state and participants of a NEO cluster
//
// Usually ClusterInfo is Master's idea about the cluster which Master shares
// with other nodes.	XXX text ok?
//
// XXX naming -> ClusterState ?  (but conflict with proto.ClusterState)
type ClusterInfo struct {
	State	ClusterState	// what is cluster currently doing: recovering/verification/service/...
	NodeTab	NodeTable	// nodes participating in the cluster
	PartTab	PartitionTable	// data space partitioning

	// XXX do we want to put data movement scheduling plans here ?
}
