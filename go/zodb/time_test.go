// Copyright (C) 2017-2019  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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

package zodb

import (
	"testing"
	"time"
)

func TestTidTime(t *testing.T) {
	var testv = []struct {tid Tid; timeStr string; timeFloat float64} {
		{0x0000000000000000, "1900-01-01 00:00:00.000000", -2208988800.000000000},
		{0x0285cbac258bf266, "1979-01-03 21:00:08.800000",   284245208.800000191},
		{0x0285cbad27ae14e6, "1979-01-03 21:01:09.300001",   284245269.300001502},
		{0x037969f722a53488, "2008-10-24 05:11:08.120000",  1224825068.120000124},
		{0x03b84285d71c57dd, "2016-07-01 09:41:50.416574",  1467366110.416574001},
		{0x03caa84275fc1166, "2018-10-01 16:34:27.652650",  1538411667.652650118},
	}

	for _, tt := range testv {
		tidtime := tt.tid.Time()
		timeStr := tidtime.String()
		if timeStr != tt.timeStr {
			t.Errorf("%v: timeStr = %q  ; want %q", tt.tid, timeStr, tt.timeStr)
		}

		timeFloat := float64(tidtime.UnixNano()) * 1E-9
		if timeFloat != tt.timeFloat {
			t.Errorf("%v: timeFloat = %.9f  ; want %.9f", tt.tid, timeFloat, tt.timeFloat)
		}

		locv := []*time.Location{time.UTC, time.FixedZone("UTC+4", +4*60*60)}
		for _, loc := range locv {
			tid := TidFromTime(tidtime.In(loc))
			if tid != tt.tid {
				t.Errorf("%v: ->time(%s)->tid != identity  (= %v;  δ: %s)", tt.tid, loc, tid, tt.tid - tid)
			}
		}
	}
}
