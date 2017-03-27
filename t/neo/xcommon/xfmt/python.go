package xfmt

import (
	"bytes"
	"strconv"
	"unicode/utf8"

	"lab.nexedi.com/kirr/go123/mem"
)


// TODO remove - not needed ?
// // pyQuote quotes string the way python repr(str) would do
// func pyQuote(s string) string {
// 	out := pyQuoteBytes(mem.Bytes(s))
// 	return mem.String(out)
// }
//
// func pyQuoteBytes(b []byte) []byte {
// 	buf := make([]byte, 0, (len(b) + 2) /* to reduce allocations when quoting */ * 2)
// 	return pyAppendQuoteBytes(buf, b)
// }

// AppendQuotePy appends to buf Python quoting of s
func AppendQuotePy(buf []byte, s string) []byte {
	return AppendQuotePyBytes(buf, mem.Bytes(s))
}

// AppendQuotePyBytes appends to buf Python quoting of b
func AppendQuotePyBytes(buf, b []byte) []byte {
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
			buf = append(buf, '\\', 'x', hexdigits[b[0]>>4], hexdigits[b[0]&0xf])
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
				buf = append(buf, '\\', 'x', hexdigits[b[0]>>4], hexdigits[b[0]&0xf])

			case r < utf8.RuneSelf /* RuneSelf itself is not printable */ - 1:
				// we already escaped all < RuneSelf runes
				buf = append(buf, byte(r))

			case strconv.IsPrint(r):
				// printable utf-8 characters go as is
				buf = append(buf, b[:size]...)

			default:
				// everything else goes in numeric byte escapes
				for i := 0; i < size; i++ {
					buf = append(buf, '\\', 'x', hexdigits[b[i]>>4], hexdigits[b[i]&0xf])
				}
			}
		}

		b = b[size:]
	}

	buf = append(buf, quote)
	return buf
}


// Qpy appends string quoted as Python would do
func (b *Buffer) Qpy(s string) *Buffer {
	*b = AppendQuotePy(*b, s)
	return b
}

// Qbpy appends []byte quoted as Python would do
func (b *Buffer) Qbpy(x []byte) *Buffer {
	*b = AppendQuotePyBytes(*b, x)
	return b
}
