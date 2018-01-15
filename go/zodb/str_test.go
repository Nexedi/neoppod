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

// estr returns string corresponding to error or "" for nil
func estr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestParseHex64(t *testing.T) {
	var testv = []struct {in string; out uint64; estr string} {
		{"", 0, `tid "" invalid`},
		{"0123456789abcde", 0, `tid "0123456789abcde" invalid`},
		{"0123456789abcdeq", 0, `tid "0123456789abcdeq" invalid`},
		{"0123456789abcdef", 0x0123456789abcdef, ""},
	}

	for _, tt := range testv {
		x, err := parseHex64("tid", tt.in)
		if !(x == tt.out && estr(err) == tt.estr) {
			t.Errorf("parsehex64: %v: test error:\nhave: %v %q\nwant: %v %q", tt.in, x, err, tt.out, tt.estr)
		}
	}
}

func TestParseXid(t *testing.T) {
	var testv = []struct {in string; xid Xid; estr string} {
		{"", Xid{}, `xid "" invalid`},
		{"a", Xid{}, `xid "a" invalid`},
		{"0123456789abcdef", Xid{}, `xid "0123456789abcdef" invalid`},
		{"z0123456789abcdef", Xid{}, `xid "z0123456789abcdef" invalid`},
		{"=0123456789abcdef", Xid{}, `xid "=0123456789abcdef" invalid`},
		{"<0123456789abcdef", Xid{}, `xid "<0123456789abcdef" invalid`},

		{"=0123456789abcdef|fedcba9876543210", Xid{}, `xid "=0123456789abcdef|fedcba9876543210" invalid`},
		{"<0123456789abcdef|fedcba9876543210", Xid{}, `xid "<0123456789abcdef|fedcba9876543210" invalid`},

		{"=0123456789abcdef:fedcba9876543210", Xid{}, `xid "=0123456789abcdef:fedcba9876543210" invalid`},
		{"<0123456789abcdef:fedcba9876543210", Xid{}, `xid "<0123456789abcdef:fedcba9876543210" invalid`},
		{"0123456789abcdef:fedcba9876543210", Xid{0x0123456789abcdef, 0xfedcba9876543210}, ""},
	}

	for _, tt := range testv {
		xid, err := ParseXid(tt.in)
		if !(xid == tt.xid && estr(err) == tt.estr) {
			t.Errorf("parsexid: %v: test error:\nhave: %v %q\nwant: %v %q",
				tt.in, xid, err, tt.xid, tt.estr)
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
