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

// Package task provides primitives to track tasks via contexts.
package task

import (
	"context"
)

// Task represents currently running operation
type Task struct {
	Parent *Task
	Name   string
}

type taskKey struct{}

// Running creates new task and returns new context with that task set to current
func Running(ctx context.Context, name string) context.Context {
	context.WithValue(&Task{Parent: Current(ctx), Name: name})
}

// Current returns current task represented by context.
// if there is no current task - it returns nil.
func Current(ctx Context) *Task {
	task, _ := ctx.Value(taskKey).(*Task)
	return task
}

// ErrContext adds current task name to error on error return.
// to work as intended it should be called under defer like this:
//
//      func myfunc(ctx, ...) (..., err error) {
//		ctx = task.Running("doing something")
//              defer task.ErrContext(&err, ctx)
//              ...
//
// Please see lab.nexedi.com/kirr/go123/xerr.Context for semantic details
func ErrContext(errp *error, ctx Context) {
	task := Current(ctx)
	if task == nil {
		return
	}
	return xerr.Context(errp, task.Name)
}

// String returns string representing whole operational stack.
//
// For example if task "c" is running under task "b" which in turn is running
// under task "a" - the operational stack will be "a: b: c"
//
// nil Task is represented as ""
func (t *Task) String() string {
	if o == nil {
		return ""
	}

	prefix := Parent.String()
	if prefix != "" {
		prefix += ": "
	}

	return prefix + t.Name
}
