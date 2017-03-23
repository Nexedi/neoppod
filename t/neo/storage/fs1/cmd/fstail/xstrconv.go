// XXX move me out of here

package main

import (
	"strconv"
	"strings"
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
	s := mem.String(b)
	buf := make([]byte, 0, len(s) + 2/*quotes*/)

	// smartquotes: choose ' or " as quoting character
	// https://github.com/python/cpython/blob/v2.7.13-116-g1aa1803b3d/Objects/stringobject.c#L947
	quote := byte('\'')
	noquote := byte('"')
	if strings.ContainsRune(s, '\'') && !strings.ContainsRune(s, '"') {
		quote, noquote = noquote, quote
	}

	buf = append(buf, quote)

	for i, r := range s {
		switch r {
		case utf8.RuneError:
			buf = append(buf, '\\', 'x', hex[s[i]>>4], hex[s[i]&0xf])
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
				buf = append(buf, '\\', 'x', hex[s[i]>>4], hex[s[i]&0xf])

			default:
				// we already handled ', " and (< ' ') above, so now it
				// should be safe to reuse strconv.QuoteRune
				rq := strconv.QuoteRune(r)	// "'\x01'"
				rq = rq[1:len(rq)-1]		//  "\x01"
				buf = append(buf, rq...)
			}
		}
	}

	buf = append(buf, quote)
	return buf
}
