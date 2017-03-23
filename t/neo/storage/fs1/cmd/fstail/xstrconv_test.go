// XXX move me to common place

package main

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

func TestPyQuote(t *testing.T) {
	// XXX -> global
	testv := []struct {in, quoted string} {
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
		// readable
		//{`привет мир`, `'\xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82 \xd0\xbc\xd0\xb8\xd1\x80'`},
		{`привет мир`, `'привет мир'`},

		// invalid utf-8
		{"\xd0a", `'\xd0a'`},
	}

	for _, tt := range testv {
		quoted := pyQuote(tt.in)
		if quoted != tt.quoted {
			t.Errorf("pyQuote(%q) -> %s  ; want %s", tt.in, quoted, tt.quoted)
		}
	}
}
