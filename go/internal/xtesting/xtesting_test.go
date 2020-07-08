// Copyright (C) 2020  Nexedi SA and Contributors.
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

package xtesting

import (
	"context"
	"testing"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"
)

func TestLoadDBHistory(t *testing.T) {
	X := FatalIf(t)
	data := "../../zodb/storage/fs1/testdata/1.fs"

	txnv, err := LoadDBHistory(data); X(err)

	// verify the history by running DrvTestLoad against original data.
	// FileStorage verifies its Load and Iterate without relying on LoadDBHistory.
	f, _, err := fs1.Open(context.Background(), data, &zodb.DriverOptions{ReadOnly: true}); X(err)
	defer func() {
		err := f.Close(); X(err)
	}()

	DrvTestLoad(t, f, txnv)
}
