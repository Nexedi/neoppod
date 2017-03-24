package xfmt

import (
	"encoding/hex"

	"../xslice"
)

// appendHex appends hex representation of x to b
func AppendHex(b []byte, x []byte) []byte {
	lx := hex.EncodedLen(len(x))
	lb := len(b)
	b = xslice.Grow(b, lx)
	hex.Encode(b[lb:], x)
	return b
}
