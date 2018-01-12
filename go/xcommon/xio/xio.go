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

// Package xio provides addons to standard package io.
package xio

import (
	"context"
	"io"

	"lab.nexedi.com/kirr/neo/go/xcommon/log"
	"lab.nexedi.com/kirr/neo/go/xcommon/xcontext"
)

// CloseWhenDone arranges for c to be closed either when ctx is cancelled or
// surrounding function returns.
//
// To work as intended it should be called under defer like this:
//
//	func myfunc(ctx, ...) {
//		defer xio.CloseWhenDone(ctx, c)()
//
// The error - if c.Close() returns with any - is logged.
func CloseWhenDone(ctx context.Context, c io.Closer) func() {
	return xcontext.WhenDone(ctx, func() {
		err := c.Close()
		if err != nil {
			log.Error(ctx, err)
		}
	})
}
