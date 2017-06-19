// Package fsb specializes cznic/b.Tree for FileStorage index needs.
// See gen-fsbtree for details.
package fsb

//go:generate ./gen-fsbtree

import "lab.nexedi.com/kirr/neo/go/zodb"

// comparison function for fsbTree
// kept short & inlineable
func oidCmp(a, b zodb.Oid) int {
	if a < b {
		return -1
	} else if a > b {
		return +1
	} else {
		return 0
	}
}
