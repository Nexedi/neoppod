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

func TestClientStorage(t *testing.T) {
	nlC, nlS := nodeLinkPipe()

	ctxS := context.Background()

	S := NewStorage(nil)	// TODO zodb.storage.mem
	//Serve(ctx, l, S)
	S.ServeLink(ctxS, nlS)	// XXX go

	C, err := NewClient(nlC)
	//assert err != nil

	_ = C
	_ = err

}
