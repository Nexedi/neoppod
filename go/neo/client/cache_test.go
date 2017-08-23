// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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

package client

import (
	"testing"
)

func TestCache(t *testing.T) {
	// XXX <100 <90 <80
	//	q<110	-> a) 110 <= cache.before   b) otherwise
	//	q<85	-> a) inside 90.serial..90  b) outside
	//
	// XXX cases when .serial=0 (not yet determined - 1st loadBefore is in progress)
	// XXX for every serial check before = (s-1, s, s+1)

	// merge: rce + rceNext
	//	  rcePrev + rce
	//	  rcePrev + (rce + rceNext)
}
