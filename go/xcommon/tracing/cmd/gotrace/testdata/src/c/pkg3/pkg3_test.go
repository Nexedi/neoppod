package pkg3

import "testing"

// trace import that should be added only to tests
//trace:import "a/pkg1"

func TestZzz(t *testing.T) {
	if zzz() != 2 {
		t.Fatal("zzz wrong")
	}
}
