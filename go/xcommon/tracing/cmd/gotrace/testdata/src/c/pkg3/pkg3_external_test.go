package pkg3_test

import "testing"

// trace import that should be added only to external tests
//trace:import "b/pkg2"

func TestZzzExternal(t *testing.T) {
	t.Fatal("...")
}
