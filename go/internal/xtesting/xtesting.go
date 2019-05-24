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
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"

	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/zodb"
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
		cmd := exec.Command("python", "-c", "0")
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
			cmd := exec.Command("python", "-c", "import "+pymod)
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

// ZRawObject represents raw ZODB object state.
type ZRawObject struct {
	Oid  zodb.Oid
	Data []byte // raw serialized zodb data
}

// ZPyCommitRaw commits new transaction into database @ zurl with raw data specified by objv.
//
// The commit is performed via zodbtools/py.
func ZPyCommitRaw(zurl string, at zodb.Tid, objv ...ZRawObject) (_ zodb.Tid, err error) {
	defer xerr.Contextf(&err, "%s: zpycommit @%s", zurl, at)

	// prepare text input for `zodb commit`
	zin := &bytes.Buffer{}
	fmt.Fprintf(zin, "user %q\n", "author")
	fmt.Fprintf(zin, "description %q\n", fmt.Sprintf("test commit; at=%s", at))
	fmt.Fprintf(zin, "extension %q\n", "")
	for _, obj := range objv {
		fmt.Fprintf(zin, "obj %s %d null:00\n", obj.Oid, len(obj.Data))
		zin.Write(obj.Data)
		zin.WriteString("\n")
	}
	zin.WriteString("\n")

	// run py `zodb commit`
	cmd:= exec.Command("python", "-m", "zodbtools.zodb", "commit", zurl, at.String())
	cmd.Stdin  = zin
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return zodb.InvalidTid, err
	}

	out = bytes.TrimSuffix(out, []byte("\n"))
	tid, err := zodb.ParseTid(string(out))
	if err != nil {
		return zodb.InvalidTid, fmt.Errorf("committed, but invalid output: %s", err)
	}

	return tid, nil
}
