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
	"fmt"
	"reflect"
	"testing"
)

var testv = []struct {format, xformatMeth string; value interface{}} {
	{"%c",		"Cb",		byte('A')},
	{"%c",		"C",		rune(-1)},
	{"%c",		"C",		'B'},		// 1-byte encoded
	{"%c",		"C",		'и'},		// 2-bytes encoded
	{"%c",		"C",		'\u20ac'},	// 3-bytes encoded
	{"%c",		"C",		'\U00010001'},	// 4-bytes encoded

	{"%s",		"S",		"hello"},
	{"%s",		"Sb",		[]byte("world")},
	{"%q",		"Q",		"alpha"},
	{"%q",		"Qb",		[]byte("beta")},
	{"%q",		"Qcb",		byte('D')},
	{"%q",		"Qc",		'B'},		// 1-byte encoded
	{"%q",		"Qc",		'и'},		// 2-bytes encoded
	{"%q",		"Qc",		'\u20ac'},	// 3-bytes encoded
	{"%q",		"Qc",		'\U00010001'},	// 4-bytes encoded

	{"%x",		"Xb",		[]byte("hexstring")},
	{"%x",		"Xs",		"stringhex"},
	{"%d",		"D",		12765},
	{"%d",		"D64",		int64(12764)},
	{"%x",		"X",		12789},
	{"%016x",	"X016",		uint64(124)},

	{"%v",		"V",		&stringerTest{}},
}


type stringerTest struct {
}

func (s *stringerTest) String() string {
	return string(s.XFmtString(nil))
}

func (s *stringerTest) XFmtString(b []byte) []byte {
	return append(b, `stringer test`...)
}

// verify formatting result is the same in between std fmt and xfmt
func TestXFmt(t *testing.T) {
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

func BenchmarkXFmt(b *testing.B) {
	buf := &Buffer{}

	for _, tt := range testv {
		b.Run(fmt.Sprintf("%s(%#v)", tt.format, tt.value), func (b *testing.B) {
			for i := 0; i < b.N; i++ {
				fmt.Sprintf(tt.format, tt.value)
			}
		})

		// construct methProxy for natively calling (not via reflect) method associated with tt.xformatMeth
		// (calling via reflect allocates a lot and is slow)
		// NOTE because of proxies the call is a bit slower than e.g. directly calling buf.S("...")
		var methProxy func(buf *Buffer, v interface{})

		xmeth, ok := reflect.TypeOf(buf).MethodByName(tt.xformatMeth)
		if !ok {
			b.Errorf(".%v: no such method", tt.xformatMeth)
			continue
		}

		// XXX a bit ugly -> use code generation instead?
		meth := xmeth.Func.Interface()
		switch tt.value.(type) {
		case byte:	methProxy = func(buf *Buffer, v interface{}) { meth.(func (*Buffer, byte) *Buffer)(buf, v.(byte)) }
		case rune:	methProxy = func(buf *Buffer, v interface{}) { meth.(func (*Buffer, rune) *Buffer)(buf, v.(rune)) }
		case string:	methProxy = func(buf *Buffer, v interface{}) { meth.(func (*Buffer, string) *Buffer)(buf, v.(string)) }
		case []byte:	methProxy = func(buf *Buffer, v interface{}) { meth.(func (*Buffer, []byte) *Buffer)(buf, v.([]byte)) }
		case int:	methProxy = func(buf *Buffer, v interface{}) { meth.(func (*Buffer, int) *Buffer)(buf, v.(int)) }
		case int64:	methProxy = func(buf *Buffer, v interface{}) { meth.(func (*Buffer, int64) *Buffer)(buf, v.(int64)) }
		case uint64:	methProxy = func(buf *Buffer, v interface{}) { meth.(func (*Buffer, uint64) *Buffer)(buf, v.(uint64)) }

		case *stringerTest:	methProxy = func(buf *Buffer, v interface{}) { meth.(func (*Buffer, Stringer) *Buffer)(buf, v.(Stringer)) }

		default:
			b.Fatalf("TODO add support for %T", tt.value)
		}

		b.Run(fmt.Sprintf(".%s(%#v)", tt.xformatMeth, tt.value), func (b *testing.B) {
			for i := 0; i < b.N; i++ {
				buf.Reset()
				methProxy(buf, tt.value)
			}
		})
	}
}
