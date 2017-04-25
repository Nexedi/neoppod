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

package zodb

import "testing"

func TestParseHex64(t *testing.T) {
	var testv = []struct {in string; out uint64; estr string} {
		{"", 0, `tid "" invalid`},
		{"0123456789abcde", 0, `tid "0123456789abcde" invalid`},
		{"0123456789abcdeq", 0, `tid "0123456789abcdeq" invalid`},
		{"0123456789abcdef", 0x0123456789abcdef, ""},
	}

	for _, tt := range testv {
		x, err := parseHex64("tid", tt.in)
		estr := ""
		if err != nil {
			estr = err.Error()
		}

		if !(x == tt.out && estr == tt.estr) {
			t.Errorf("parsehex64: %v: test error:\nhave: %v %q\nwant: %v %q", tt.in, x, estr, tt.out, tt.estr)
		}
	}
}

func TestParseTidRange(t *testing.T) {
	var testv = []struct {in string; tidMin, tidMax Tid; estr string} {
		{"", 0, 0, `tid range "" invalid`},
		{".", 0, 0, `tid range "." invalid`},
		{"..", 0, TidMax, ""},
		{"0123456789abcdef..", 0x0123456789abcdef, TidMax, ""},
		{"..0123456789abcdef", 0, 0x0123456789abcdef, ""},
	}

	for _, tt := range testv {
		tmin, tmax, err := ParseTidRange(tt.in)
		estr := ""
		if err != nil {
			estr = err.Error()
		}

		if !(tmin == tt.tidMin && tmax == tt.tidMax && estr == tt.estr) {
			t.Errorf("parseTidRange: %v: test error:\nhave: %v %v %q\nwant: %v %v %q", tt.in,
				tmin, tmax, estr, tt.tidMin, tt.tidMax, tt.estr)
		}
	}
}
