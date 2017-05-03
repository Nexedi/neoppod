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
// test interaction between nodes

import (
	"context"
	"testing"
)

// basic interaction between Client -- Storage
func TestClientStorage(t *testing.T) {
	Cnl, Snl := nodeLinkPipe()
	wg := WorkGroup()

	Sctx, Scancel := context.WithCancel(context.Background())

	S := NewStorage(nil)	// TODO zodb.storage.mem
	wg.Gox(func() {
		S.ServeLink(Sctx, Snl)
		// XXX + test error return
	})

	C, err := NewClient(Cnl)
	if err != nil {
		t.Fatalf("creating/identifying client: %v", err)
	}

	lastTid, err := C.LastTid()
	if !(lastTid == 111 && err == nil) {	// XXX 111
		t.Fatalf("C.LastTid -> %v, %v  ; want %v, nil", lastTid, err, 111, err)
	}

	// shutdown storage
	// XXX wait for S to shutdown + verify shutdown error
	Scancel()

	xwait(wg)
}
