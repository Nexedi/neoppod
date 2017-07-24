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

package fs1tools

//go:generate sh -c "python2 -m ZODB.scripts.fstail -n 1000000 ../testdata/1.fs >testdata/1.fstail.ok"

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/sergi/go-diff/diffmatchpatch"
)

// XXX -> xtesting ?
func loadFile(t *testing.T, path string) string {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// XXX -> xtesting ?
// XXX dup in zodbdump_test.go
func diff(a, b string) string {
	dmp := diffmatchpatch.New()
	diffv := dmp.DiffMain(a, b, /*checklines=*/false)
	return dmp.DiffPrettyText(diffv)
}

func TestTail(t *testing.T) {
	buf := bytes.Buffer{}

	err := Tail(&buf, "../testdata/1.fs", 1000000)
	if err != nil {
		t.Fatal(err)
	}

	dumpOk := loadFile(t, "testdata/1.fstail.ok")

	if dumpOk != buf.String() {
		t.Errorf("dump different:\n%v", diff(dumpOk, buf.String()))
	}
}

func BenchmarkTail(b *testing.B) {
	// FIXME small testdata/1.fs is not representative for benchmarking
	for i := 0; i < b.N; i++ {
		err := Tail(ioutil.Discard, "../testdata/1.fs", 1000000)
		if err != nil {
			b.Fatal(err)
		}
	}
}
