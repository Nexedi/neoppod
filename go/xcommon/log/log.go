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

// Package log provides logging with severity levels and tasks integration
//
// XXX inspired by cockrach
package log

import (
	"github.com/golang/glog"

	"lab.nexedi.com/kirr/neo/go/xcommon/task"
)

// taskPrefix returns prefix associated to current operational task stack to put to logs
func taskPrefix(ctx) string {
	s := task.Current(ctx).String()
	if s != "" {
		s += ": "
	}
	return s
}


type Depth int

func (d Depth) Infof(ctx context.Context, format string, argv ...interface{}) {
	// XXX avoid formatting if logging severity disables info
	glog.InfoDepth(d+1, taskPrefix(ctx) + fmt.Sprintf(format, argv)
}

func Infof(ctx context.Context, format string, argv ...interface{}) {
	Depth(1).Infof(ctx, format, argv)
}


// TODO Warningf, Errorf, ...
