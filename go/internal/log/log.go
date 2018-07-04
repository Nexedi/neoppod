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

// Package log provides logging with severity levels and tasks integration.
//
// XXX inspired by cockroach
// XXX just use cockroach/util/log directly?
package log

import (
	"context"
	"fmt"

	"github.com/golang/glog"

	"lab.nexedi.com/kirr/neo/go/internal/xcontext/task"
)

// withTask prepends string describing current operational task stack to argv and returns it
// handy to use this way:
//
//	func info(ctx, argv ...interface{}) {
//		glog.Info(withTask(ctx, argv...)...)
//	}
//
// see https://golang.org/issues/21388
func withTask(ctx context.Context, argv ...interface{}) []interface{} {
	task := task.Current(ctx).String()
	if task == "" {
		return argv
	}

	if len(argv) != 0 {
		task += ": "
	}

	return append([]interface{}{task}, argv...)
}



type Depth int

func (d Depth) Info(ctx context.Context, argv ...interface{}) {
	// XXX avoid task formatting if logging severity disabled
	glog.InfoDepth(int(d+1), withTask(ctx, argv...)...)
}

func (d Depth) Infof(ctx context.Context, format string, argv ...interface{}) {
	// XXX avoid formatting if logging severity disabled
	glog.InfoDepth(int(d+1), withTask(ctx, fmt.Sprintf(format, argv...))...)
}

func (d Depth) Warning(ctx context.Context, argv ...interface{}) {
	glog.WarningDepth(int(d+1), withTask(ctx, argv...)...)
}

func (d Depth) Warningf(ctx context.Context, format string, argv ...interface{}) {
	glog.WarningDepth(int(d+1), withTask(ctx, fmt.Sprintf(format, argv...))...)
}

func (d Depth) Error(ctx context.Context, argv ...interface{}) {
	glog.ErrorDepth(int(d+1), withTask(ctx, argv...)...)
}

func (d Depth) Errorf(ctx context.Context, format string, argv ...interface{}) {
	glog.ErrorDepth(int(d+1), withTask(ctx, fmt.Sprintf(format, argv...))...)
}

func (d Depth) Fatal(ctx context.Context, argv ...interface{}) {
	glog.FatalDepth(int(d+1), withTask(ctx, argv...)...)
}

func (d Depth) Fatalf(ctx context.Context, format string, argv ...interface{}) {
	glog.FatalDepth(int(d+1), withTask(ctx, fmt.Sprintf(format, argv...))...)
}


func Info(ctx context.Context, argv ...interface{})	{ Depth(1).Info(ctx, argv...) }
func Warning(ctx context.Context, argv ...interface{})	{ Depth(1).Warning(ctx, argv...) }
func Error(ctx context.Context, argv ...interface{})	{ Depth(1).Error(ctx, argv...) }
func Fatal(ctx context.Context, argv ...interface{})	{ Depth(1).Fatal(ctx, argv...) }

func Infof(ctx context.Context, format string, argv ...interface{}) {
	Depth(1).Infof(ctx, format, argv...)
}

func Warningf(ctx context.Context, format string, argv ...interface{}) {
	Depth(1).Warningf(ctx, format, argv...)
}

func Errorf(ctx context.Context, format string, argv ...interface{}) {
	Depth(1).Errorf(ctx, format, argv...)
}

func Fatalf(ctx context.Context, format string, argv ...interface{}) {
	Depth(1).Fatalf(ctx, format, argv...)
}

func Flush()	{ glog.Flush() }
