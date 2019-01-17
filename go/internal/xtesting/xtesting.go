// Copyright (C) 2017-2019  Nexedi SA and Contributors.
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

// Package xtesting provides addons to std package testing.
package xtesting

import (
	"os/exec"
	"sync"
	"testing"
)

var (
	pyMu   sync.Mutex
	pyHave = map[string]bool{} // {} pymod -> y/n  ; ".python" indicates python presence
)

// NeedPy skips current test if python and specified modules are not available.
//
// For example
//
//	xtesting.NeedPy(t)
//
// would check for python presence, and
//
//	xtesting.NeedPy(t, "ZODB", "golang.strconv")
//
// would check if all python and ZODB and golang.strconv python modules are available.
func NeedPy(t testing.TB, modules ...string) {
	pyMu.Lock()
	defer pyMu.Unlock()

	// verify if python is present
	havePy, know := pyHave[".python"]
	if !know {
		cmd := exec.Command("python2", "-c", "0")
		err := cmd.Run()
		havePy = (err == nil)
		pyHave[".python"] = havePy
	}

	if !havePy {
		t.Skipf("skipping: python is not availble")
	}

	var donthave []string
	for _, pymod := range modules {
		have, know := pyHave[pymod]
		if !know {
			cmd := exec.Command("python2", "-c", "import "+pymod)
			err := cmd.Run()
			have = (err == nil)
			pyHave[pymod] = have
		}
		if !have {
			donthave = append(donthave, pymod)
		}
	}

	if len(donthave) != 0 {
		t.Skipf("skipping: the following python modules are not available: %s", donthave)
	}

	// we verified everything - now it is ok not to skip.
	return
}
