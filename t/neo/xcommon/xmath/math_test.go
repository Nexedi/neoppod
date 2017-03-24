package xmath

import (
	"testing"
)

func TestCeilPow2(t *testing.T) {
	testv := []struct {in, out uint64} {
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{5, 8},
		{6, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{10, 16},
		{11, 16},
		{12, 16},
		{13, 16},
		{14, 16},
		{15, 16},
		{16, 16},
		{1<<62 - 1, 1<<62},
		{1<<62, 1<<62},
		{1<<62+1, 1<<63},
		{1<<63 - 1, 1<<63},
		{1<<63, 1<<63},
	}

	for _, tt := range testv {
		out := CeilPow2(tt.in)
		if out != tt.out {
			t.Errorf("CeilPow(%v) -> %v  ; want %v", tt.in, out, tt.out)
		}
	}

}
