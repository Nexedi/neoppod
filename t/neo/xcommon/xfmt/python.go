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

// bytesContainsByte is like bytes.ContainsRune but a bit faster
func bytesContainsByte(s []byte, c byte) bool {
	return bytes.IndexByte(s, c) >= 0
}

// AppendQuotePy appends to buf Python quoting of s
func AppendQuotePy(buf []byte, s string) []byte {
	return AppendQuotePyBytes(buf, mem.Bytes(s))
}

// AppendQuotePyBytes appends to buf Python quoting of b
func AppendQuotePyBytes(buf, b []byte) []byte {
	// smartquotes: choose ' or " as quoting character
	// https://github.com/python/cpython/blob/v2.7.13-116-g1aa1803b3d/Objects/stringobject.c#L947
	quote := byte('\'')
	if bytesContainsByte(b, '\'') && !bytesContainsByte(b, '"') {
		quote = '"'
	}

	buf = append(buf, quote)

	for len(b) > 0 {
		c := b[0]
		switch {
		// fast path - ASCII only - trying to avoid UTF-8 decoding
		case c < utf8.RuneSelf:
			switch c {
				case '\\', quote:
					buf = append(buf, '\\', c)

				// NOTE python converts to \<letter> only \t \n \r  (not e.g. \v)
				// https://github.com/python/cpython/blob/v2.7.13-116-g1aa1803b3d/Objects/stringobject.c#L963
				case '\t':
					buf = append(buf, `\t`...)
				case '\n':
					buf = append(buf, `\n`...)
				case '\r':
					buf = append(buf, `\r`...)

				default:
					if c < ' ' || c == '\x7f' /* the only non-printable ASCII character > space */  {
						// we already converted to \<letter> what python represents as such above
						// everything else goes in numeric byte escapes
						buf = append(buf, '\\', 'x', hexdigits[c>>4], hexdigits[c&0xf])
					} else {
						// printable ASCII
						buf = append(buf, c)
					}
			}

			b = b[1:]

		// slow path - full UTF-8 decoding
		default:
			r, size := utf8.DecodeRune(b)

			switch r {
			case utf8.RuneError:
				buf = append(buf, '\\', 'x', hexdigits[c>>4], hexdigits[c&0xf])

			default:
				switch {
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
