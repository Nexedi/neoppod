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

package xfmt

import (
	"testing"
)

// byterange returns []byte with element [start,stop)
func byterange(start, stop byte) []byte {
	b := make([]byte, 0, stop-start)
	for ; start < stop; start++ {
		b = append(b, start)
	}
	return b
}

var pyQuoteTestv = []struct {in, quoted string} {
	// empty
	{``, `''`},

	// special characters
	{string(byterange(0, 32)), `'\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f'`},

	// " vs '
	{`hello world`, `'hello world'`},
	{`hello ' world`, `"hello ' world"`},
	{`hello ' " world`, `'hello \' " world'`},

	// \
	{`hello \ world`, `'hello \\ world'`},

	// utf-8
	// XXX python escapes non-ascii, but since FileStorage connot
	// commit such strings we take the freedom and output them as
	// readable.
	//{`привет мир`, `'\xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82 \xd0\xbc\xd0\xb8\xd1\x80'`},
	{`привет мир`, `'привет мир'`},

	// invalid utf-8
	{"\xd0a", `'\xd0a'`},

	// non-printable utf-8
	{"\u007f\u0080\u0081\u0082\u0083\u0084\u0085\u0086\u0087", `'\x7f\xc2\x80\xc2\x81\xc2\x82\xc2\x83\xc2\x84\xc2\x85\xc2\x86\xc2\x87'`},
}

func TestPyQuote(t *testing.T) {
	buf := []byte{}
	for _, tt := range pyQuoteTestv {
		buf = buf[:0]
		buf = AppendQuotePy(buf, tt.in)
		quoted := string(buf)
		if quoted != tt.quoted {
			t.Errorf("pyQuote(%q) ->\nhave: %s\nwant: %s", tt.in, quoted, tt.quoted)
		}
	}
}

func BenchmarkPyQuote(b *testing.B) {
	buf := []byte{}

	for i := 0; i < b.N; i++ {
		for _, tt := range pyQuoteTestv {
			buf = buf[:0]
			buf = AppendQuotePy(buf, tt.in)
		}
	}
}
