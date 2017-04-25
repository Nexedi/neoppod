package zodb

import (
	"testing"
)


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
