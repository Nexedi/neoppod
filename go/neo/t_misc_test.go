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

import (
	"context"
	"math"

	zfs1 "lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"
	bfs1 "lab.nexedi.com/kirr/neo/go/neo/storage/fs1"
	"lab.nexedi.com/kirr/go123/exc"
)

// XXX dup from connection_test
func xwait(w interface { Wait() error }) {
	err := w.Wait()
	exc.Raiseif(err)
}

func gox(wg interface { Go(func() error) }, xf func()) {
	wg.Go(exc.Funcx(xf))
}

func xfs1stor(path string) *zfs1.FileStorage {
	stor, err := zfs1.Open(bg, path)
	exc.Raiseif(err)
	return stor
}

func xfs1back(path string) *bfs1.Backend {
	back, err := bfs1.Open(bg, path)
	exc.Raiseif(err)
	return back
}

var bg = context.Background()

// vclock is a virtual clock
type vclock struct {
	t float64
}

func (c *vclock) monotime() float64 {
	c.t += 1E-2
	return c.t
}

func (c *vclock) tick() {	// XXX do we need tick?
	t := math.Ceil(c.t)
	if !(t > c.t) {
		t += 1
	}
	c.t = t
}
