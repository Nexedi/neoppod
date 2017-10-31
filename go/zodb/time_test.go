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

package zodb

import "testing"

func TestTidTime(t *testing.T) {
	var testv = []struct {tid Tid; timeStr string} {
		{0x0000000000000000, "1900-01-01 00:00:00.000000"},
		{0x0285cbac258bf266, "1979-01-03 21:00:08.800000"},
		{0x0285cbad27ae14e6, "1979-01-03 21:01:09.300001"},
		{0x037969f722a53488, "2008-10-24 05:11:08.120000"},
		{0x03b84285d71c57dd, "2016-07-01 09:41:50.416574"},
	}

	for _, tt := range testv {
		timeStr := tt.tid.Time().String()
		if timeStr != tt.timeStr {
			t.Errorf("%v: timeStr = %q  ; want %q", tt.tid, timeStr, tt.timeStr)
		}
	}
}
