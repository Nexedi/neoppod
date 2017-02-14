package fsb

import "../../../zodb"

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
