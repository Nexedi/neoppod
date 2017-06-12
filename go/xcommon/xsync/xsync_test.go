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

package xsync

import (
	"context"
	"testing"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/my"
)

func TestWorkGroup(t *testing.T) {
	g := WorkGroup{}

	g.Gox(func() {
		exc.Raise(1)
	})

	err := g.Wait()

	e, ok := err.(*exc.Error)
	want := my.FuncName() + ".func1: 1"
	if !(ok && e.Error() == want) {
		t.Fatalf("gox:\nhave: %v\nwant: %v", err, want)
	}

	g2, ctx := WorkGroupCtx(context.Background())
	g2.Gox(func() {
		exc.Raise(2)
	})

	<-ctx.Done()
}
