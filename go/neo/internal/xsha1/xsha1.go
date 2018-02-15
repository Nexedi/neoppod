// Copyright (C) 2017-2018  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

// Package xsha1, similarly to crypto/sha1, provides SHA1 computation, but
// makes it a noop if requested  from environment.
package xsha1

import (
	"crypto/sha1"
	"fmt"
	"os"
)

// XXX for benchmarking: how much sha1 computation takes time from latency
var Skip bool
func init() {
	if os.Getenv("X_NEOGO_SHA1_SKIP") == "y" {
		fmt.Fprintf(os.Stderr, "# NEO/go (%s): skipping SHA1 computations\n", os.Args[0])
		Skip = true
	}
}

func Sum(b []byte) [sha1.Size]byte {
	if !Skip {
		return sha1.Sum(b)
	}

	return [sha1.Size]byte{} // all 0
}
