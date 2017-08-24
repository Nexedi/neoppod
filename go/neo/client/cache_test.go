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

package client

import (
	"sort"
	"testing"

	"lab.nexedi.com/kirr/neo/go/zodb"
)

// tStorage implements read-only storage for cache testing
type tStorage struct {
	//txnv []tTxnRecord	// transactions;  .tid↑

	// oid -> [](.serial, .data)
	dataMap map[zodb.Oid][]tOidData	// with .serial↑
}

// data for oid for 1 revision
type tOidData struct {
	serial zodb.Tid
	data   []byte
}

var tstor = &tStorage{
	dataMap: map[zodb.Oid][]tOidData{
		1: {
			{3, []byte("hello")},
			{7, []byte("world")},
		},
	},
}

/*
type tTxnRecord struct {
	tid	zodb.Tid

	// data records for oid changed in transaction
	// .oid↑
	datav []tDataRecord
}

type tDataRecord struct {
	oid	zodb.Oid
	data	[]byte
}

	if xid.TidBefore {
		// find max txn with .tid < xid.Tid
		n := len(s.txnv)
		i := n - 1 - sort.Search(n, func(i int) bool {
			return s.txnv[n - 1 - i].tid < xid.Tid
		})
		if i == -1 {
			// XXX xid.Tid < all .tid - no such transaction
		}
	}
*/

func (s *tStorage) Load(xid zodb.Xid) (data []byte, serial zodb.Tid, err error) {
	tid := xid.Tid
	if xid.TidBefore {
		tid++		// XXX overflow
	}

	datav := s.dataMap[xid.Oid]
	if datav == nil {
		return nil, 0, &zodb.ErrOidMissing{xid.Oid}
	}

	// find max entry with .serial < tid
	n := len(datav)
	i := n - 1 - sort.Search(n, func(i int) bool {
		return datav[n - 1 - i].serial < tid
	})
	if i == -1 {
		// tid < all .serial - no such transaction
		return nil, 0, &zodb.ErrXidMissing{xid}
	}

	// check we have exact match if it was loadSerial
	if xid.TidBefore && datav[i].serial != xid.Tid {
		return nil, 0, &zodb.ErrXidMissing{xid}
	}

	return datav[i].data, datav[i].serial, nil
}

func TestCache(t *testing.T) {
	// XXX <100 <90 <80
	//	q<110	-> a) 110 <= cache.before   b) otherwise
	//	q<85	-> a) inside 90.serial..90  b) outside
	//
	// XXX cases when .serial=0 (not yet determined - 1st loadBefore is in progress)
	// XXX for every serial check before = (s-1, s, s+1)

	// merge: rce + rceNext
	//	  rcePrev + rce
	//	  rcePrev + (rce + rceNext)
}
