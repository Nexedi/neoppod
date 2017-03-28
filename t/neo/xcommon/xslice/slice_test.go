package xslice

import (
	"bytes"
	"reflect"
	"testing"
	"unsafe"
)

// aliases returns whether two slice memory is aliased
func aliases(b1, b2 []byte) bool {
	s1 := (*reflect.SliceHeader)(unsafe.Pointer(&b1))
	s2 := (*reflect.SliceHeader)(unsafe.Pointer(&b2))
	return s1.Data == s2.Data
}

func TestSlice(t *testing.T) {
	s := make([]byte, 0, 10)

	testv := []struct {op func([]byte, int) []byte; n, Len, Cap int; aliased bool; content []byte} {
		// op,    n, Len, Cap, aliased, content
		{Grow,     5,  5, 10, true,  []byte{0,0,0,0,0}},

		// here "Hello" is assigned
		{Grow,     6, 11, 16, false, []byte("Hello\x00\x00\x00\x00\x00\x00")},
		{Resize,   8,  8, 16, true,  []byte("Hello\x00\x00\x00")},
		{Resize,  17, 17, 32, false, []byte("Hello\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")},
		{Realloc, 16, 16, 32, true,  []byte("Hello\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")},
		{Realloc, 33, 33, 64, false, make([]byte, 33)},
	}

	for i, tt := range testv {
		sprev := s
		s = tt.op(s, tt.n)

		if !(len(s) == tt.Len && cap(s) == tt.Cap && bytes.Equal(s, tt.content)) {
			t.Fatalf("step %d: %v: unexpected slice state: %v", i, tt, s)
		}

		if !(aliases(s, sprev) == tt.aliased) {
			t.Fatalf("step %d: %v: unexpected slice aliasing: %v", aliases(s, sprev))
		}


		// assign data after fisrt iteration
		if i == 0 {
			copy(s, "Hello")
		}
	}
}
