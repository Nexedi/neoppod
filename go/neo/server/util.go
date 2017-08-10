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

package server
// misc utilities

import (
	"context"
	"fmt"
	"io"

	"lab.nexedi.com/kirr/neo/go/xcommon/task"
	"lab.nexedi.com/kirr/neo/go/xcommon/log"
)


// XXX -> task  (and current task -> taskctx) ?

// running is syntactic sugar to push new task to operational stack, log it and
// adjust error return with task prefix.
//
// use like this:
//
//	defer running(&ctx, "my task")(&err)
func running(ctxp *context.Context, name string) func(*error) {
	return _running(ctxp, name)
}

// runningf is running cousin with formatting support
func runningf(ctxp *context.Context, format string, argv ...interface{}) func(*error) {
	return _running(ctxp, fmt.Sprintf(format, argv...))
}

func _running(ctxp *context.Context, name string) func(*error) {
	ctx := task.Running(*ctxp, name)
	*ctxp = ctx
	log.Depth(2).Info(ctx, "start")

	return func(errp *error) {
		if *errp != nil {
			// XXX is it good idea to log to error here? (not in above layer)
			// XXX what is error here could be not so error above
			// XXX or we still want to log all errors - right?
			log.Depth(1).Error(ctx, *errp)
		} else {
			log.Depth(1).Info(ctx, "ok")
		}

		// XXX do we need vvv if we log it anyway ^^^ ?
		// NOTE not *ctxp here - as context pointed by ctxp could be
		// changed when this deferred function is run
		task.ErrContext(errp, ctx)
	}
}


// lclose closes c and logs closing error if there was any.
// the error is otherwise ignored
func lclose(ctx context.Context, c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Error(ctx, err)
	}
}
