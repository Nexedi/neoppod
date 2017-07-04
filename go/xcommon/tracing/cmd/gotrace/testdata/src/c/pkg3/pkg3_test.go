package pkg3

import "testing"

// trace import that should be added only to tests, and under specified package name
//trace:import aaa1 "a/pkg1"

func TestZzz(t *testing.T) {
	if zzz() != 2 {
		t.Fatal("zzz wrong")
	}
}
