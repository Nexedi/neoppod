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
//go:generate sh -c "python2 -c 'from ZODB.FileStorage import fsdump; fsdump.main()' ../testdata/1.fs >testdata/1.fsdump.ok"
//go:generate sh -c "python2 -c 'from ZODB.FileStorage.fsdump import Dumper; import sys; d = Dumper(sys.argv[1]); d.dump()' ../testdata/1.fs >testdata/1.fsdumpv.ok"

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"

	"github.com/kylelemons/godebug/diff"
)

// XXX -> xtesting ?
func loadFile(t *testing.T, path string) string {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func testDump(t *testing.T, dir fs1.IterDir, d Dumper) {
	buf := bytes.Buffer{}

	err := Dump(&buf, "../testdata/1.fs", dir, d)
	if err != nil {
		t.Fatalf("%s: %v", d.DumperName(), err)
	}

	dumpOk := loadFile(t, fmt.Sprintf("testdata/1.%s.ok", d.DumperName()))

	if dumpOk != buf.String() {
		t.Errorf("%s: dump different:\n%v", d.DumperName(), diff.Diff(dumpOk, buf.String()))
	}
}

func TestFsDump(t *testing.T)	{ testDump(t, fs1.IterForward,  &DumperFsDump{}) }
func TestFsDumpv(t *testing.T)	{ testDump(t, fs1.IterForward,  &DumperFsDumpVerbose{}) }
func TestFsTail(t *testing.T)	{ testDump(t, fs1.IterBackward, &DumperFsTail{Ntxn: 1000000}) }

func BenchmarkTail(b *testing.B) {
	// FIXME small testdata/1.fs is not representative for benchmarking
	for i := 0; i < b.N; i++ {
		err := Dump(ioutil.Discard, "../testdata/1.fs", fs1.IterBackward, &DumperFsTail{Ntxn: 1000000})
		if err != nil {
			b.Fatal(err)
		}
	}
}
