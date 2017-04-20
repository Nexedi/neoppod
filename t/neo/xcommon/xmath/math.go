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

// XXX check on go18

// Package xmath provides addons to std math package
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
