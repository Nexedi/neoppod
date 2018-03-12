// Copyright (C) 2018  Nexedi SA and Contributors.
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

package sqlite

//go:generate ./py/gen-testdata

import (
	"context"
	"testing"

	"github.com/pkg/errors"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

// TODO verify data can be read as the same

var bg = context.Background()

func BenchmarkLoad(b *testing.B) {
	back, err := Open("testdata/1.sqlite")
	exc.Raiseif(err)

	defer func() {
		err := back.Close()
		exc.Raiseif(err)
	}()

	lastTid, err := back.LastTid(bg)
	exc.Raiseif(err)

	xid := zodb.Xid{Oid: 0, At: lastTid}

	b.ResetTimer()

loop:
	for i := 0; i < b.N; i++ {
		obj, err := back.Load(bg, xid)
		if err != nil {
			switch errors.Cause(err).(type) {
			case *zodb.NoObjectError:
				xid.Oid = 0	// last object was read -> cycle back to beginning
				continue loop

			case *zodb.NoDataError:
				panic(err)	// XXX object was deleted


			default:
				b.Fatal(err)
			}
		}

		buf := obj.Data

		if obj.Compression {
			b.Fatal("compression was used")
		}

		buf.XRelease()
	}
}
