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

// Package xsync provides addons to packages "sync" and "golang.org/x/sync"
package xsync

import (
	"context"
	"golang.org/x/sync/errgroup"

	"lab.nexedi.com/kirr/go123/exc"
)

// WorkGroup is like x/sync/errgroup.Group but also supports exceptions
type WorkGroup struct {
	errgroup.Group
}

// Gox calls the given function in a new goroutine and handles exceptions
//
// it translates exception raised, if any, to as if it was regular error
// returned for a function under Go call.
//
// see errgroup.Group.Go documentation for details on how error from spawned
// goroutines are handled group-wise.
func (g *WorkGroup) Gox(xf func()) {
	g.Go(func() error {
		return exc.Runx(xf)
	})
}

// WorkGroupCtx returns new WorkGroup and associated context derived from ctx
// see errgroup.WithContext for semantic description and details.
func WorkGroupCtx(ctx context.Context) (*WorkGroup, context.Context) {
	g, ctx := errgroup.WithContext(ctx)
	return &WorkGroup{*g}, ctx
}
