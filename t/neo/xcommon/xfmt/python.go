package xmft

import (
	"bytes"
	"strconv"
	"unicode/utf8"

	"lab.nexedi.com/kirr/go123/mem"
)


const hex = "0123456789abcdef"

// pyQuote quotes string the way python repr(str) would do
func pyQuote(s string) string {
	out := pyQuoteBytes(mem.Bytes(s))
	return mem.String(out)
}

func pyQuoteBytes(b []byte) []byte {
	buf := make([]byte, 0, (len(b) + 2) /* to reduce allocations when quoting */ * 2)
	return pyAppendQuoteBytes(buf, b)
}

func pyAppendQuoteBytes(buf, b []byte) []byte {
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

			case r < utf8.RuneSelf /* RuneSelf itself is not printable */ - 1:
				// we already escaped all < RuneSelf runes
				buf = append(buf, byte(r))

			case strconv.IsPrint(r):
				// printable utf-8 characters go as is
				buf = append(buf, b[:size]...)

			default:
				// everything else goes in numeric byte escapes
				for i := 0; i < size; i++ {
					buf = append(buf, '\\', 'x', hex[b[i]>>4], hex[b[i]&0xf])
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
	*b = ...	// TODO
	return b
}

// Qpyb appends []byte quoted as Python would do
func (b *Buffer) Qpyb(x []byte) *Buffer {
	*b = ...	// TODO
	return b
}
