// TODO copyright/license

package xfmt

import (
	"fmt"
	"reflect"
	"testing"
)

// verify formatting result is the same in between std fmt and xfmt
func TestXFmt(t *testing.T) {
	testv := []struct {format, xformatMeth string; value interface{}} {
		{"%c",		"Cb",		byte('A')},
		{"%c",		"C",		rune(-1)},
		{"%c",		"C",		'B'},		// 1-byte encoded
		{"%c",		"C",		'и'},		// 2-bytes encoded
		{"%c",		"C",		'\u20ac'},	// 3-bytes encoded
		{"%c",		"C",		'\U00010001'},	// 4-bytes encoded

		// TODO %q qb qr qs qcb qc

		{"%s",		"S",		"hello"},
		{"%s",		"Sb",		[]byte("world")},
		{"%x",		"Xb",		[]byte("hexstring")},
		{"%x",		"Xs",		"stringhex"},
		{"%d",		"D",		12765},
		{"%x",		"X",		12789},
		{"%016x",	"X016",		uint64(124)},

		// TODO .V
	}

	buf := &Buffer{}
	xbuf := reflect.ValueOf(buf)

	for _, tt := range testv {
		// result via fmt
		resFmt := fmt.Sprintf(tt.format, tt.value)

		// result via xfmt (via reflect.Call)
		buf.Reset()

		xmeth := xbuf.MethodByName(tt.xformatMeth)
		if !xmeth.IsValid() {
			t.Errorf(".%v: no such method", tt.xformatMeth)
			continue
		}

		xargv := []reflect.Value{reflect.ValueOf(tt.value)}

		xretv := []reflect.Value{}
		callOk := false
		func () {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%v: panic: %v", tt, r)
				}
			}()
			xretv = xmeth.Call(xargv)
			callOk = true
		}()
		if !callOk {
			continue
		}

		// check all formatters return pointer to the same buf
		// (this way it is handy to do .S("hello ") .X016(123) .V(zzz) ...
		if !(len(xretv) == 1 && xretv[0].Interface() == buf) {
			t.Errorf(".%v: returned %#v  ; want %#v", tt.xformatMeth, xretv[0].Interface(), buf)
			continue
		}

		resXFmt := string(*buf)

		// results must be the same
		if resFmt != resXFmt {
			t.Errorf(".%v(%v) -> %q   !=   printf(%q) -> %q",
				tt.xformatMeth, tt.value, resXFmt, tt.format, resFmt)
		}
	}
}

func BenchmarkFmt(t *testing.T) {
	// TODO
}

func BenchmarkXFmt(t *testing.T) {
	// TODO
}
