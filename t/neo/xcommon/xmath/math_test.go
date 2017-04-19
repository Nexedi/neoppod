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

package xmath

import (
	"testing"
)

func TestCeilPow2(t *testing.T) {
	testv := []struct {in, out uint64} {
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{5, 8},
		{6, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{10, 16},
		{11, 16},
		{12, 16},
		{13, 16},
		{14, 16},
		{15, 16},
		{16, 16},
		{1<<62 - 1, 1<<62},
		{1<<62, 1<<62},
		{1<<62+1, 1<<63},
		{1<<63 - 1, 1<<63},
		{1<<63, 1<<63},
	}

	for _, tt := range testv {
		out := CeilPow2(tt.in)
		if out != tt.out {
			t.Errorf("CeilPow(%v) -> %v  ; want %v", tt.in, out, tt.out)
		}
	}

}
