// Package xmath provides addons over std math package	XXX text
package xmath

import (
	"math/bits"
)

// CeilPow2 returns minimal y >= x, such that y = 2^i
// XXX naming to reflect it works on uint64 ? CeilPow264 looks ambigous
func CeilPow2(x uint64) uint64 {
	switch bits.OnesCount64(x) {
	case 0, 1:
		return x // either 0 or 2^i already
	default:
		return 1 << uint(bits.Len64(x))
	}
}

// XXX if needed: NextPow2 (y > x, such that y = 2^i) is
//	1 << bits.Len64(x)
