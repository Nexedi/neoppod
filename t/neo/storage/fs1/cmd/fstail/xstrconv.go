// XXX move me out of here

package main

import (
	"bytes"
	"strconv"
	"unicode/utf8"

	"lab.nexedi.com/kirr/go123/mem"
)

// pyQuote quotes string the way python repr(str) would do
func pyQuote(s string) string {
	out := pyQuoteBytes(mem.Bytes(s))
	return mem.String(out)
}


const hex = "0123456789abcdef"

func pyQuoteBytes(b []byte) []byte {
	buf := make([]byte, 0, (len(b) + 2) /* to reduce allocations when quoting */ * 2)

	// smartquotes: choose ' or " as quoting character
	// https://github.com/python/cpython/blob/v2.7.13-116-g1aa1803b3d/Objects/stringobject.c#L947
	quote := byte('\'')
	noquote := byte('"')
	if bytes.ContainsRune(b, '\'') && !bytes.ContainsRune(b, '"') {
		quote, noquote = noquote, quote
	}

	buf = append(buf, quote)

	for len(b) > 0 {
		r, size := utf8.DecodeRune(b)

		switch r {
		case utf8.RuneError:
			buf = append(buf, '\\', 'x', hex[b[0]>>4], hex[b[0]&0xf])
		case '\\', rune(quote):
			buf = append(buf, '\\', byte(r))
		case rune(noquote):
			buf = append(buf, noquote)

		// NOTE python converts to \<letter> only \t \n \r  (not e.g. \v)
		// https://github.com/python/cpython/blob/v2.7.13-116-g1aa1803b3d/Objects/stringobject.c#L963
		case '\t':
			buf = append(buf, `\t`...)
		case '\n':
			buf = append(buf, `\n`...)
		case '\r':
			buf = append(buf, `\r`...)

		default:
			switch {
			case r < ' ':
				// we already converted to \<letter> what python represents as such above
				buf = append(buf, '\\', 'x', hex[b[0]>>4], hex[b[0]&0xf])

			case strconv.IsPrint(r):
				// shortcut to avoid calling QuoteRune
				buf = append(buf, b[:size]...)

			default:
				// we already handled ', " and (< ' ') above, so now it
				// should be safe to reuse strconv.QuoteRune
				rq := strconv.QuoteRune(r)	// "'\x01'"
				rq = rq[1:len(rq)-1]		//  "\x01"
				buf = append(buf, rq...)
			}
		}

		b = b[size:]
	}

	buf = append(buf, quote)
	return buf
}
