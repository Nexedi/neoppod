// Copyright (C) 2016-2019  Nexedi SA and Contributors.
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

package zodbtools

//go:generate sh -c "python2 -m zodbtools.zodb dump ../../zodb/storage/fs1/testdata/1.fs >testdata/1.zdump.pyok"
//go:generate sh -c "python2 -m zodbtools.zodb dump ../../zodb/storage/fs1/testdata/empty.fs >testdata/empty.zdump.pyok"

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"regexp"
	"testing"

	"lab.nexedi.com/kirr/neo/go/zodb"
	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"

	"github.com/kylelemons/godebug/diff"
	"lab.nexedi.com/kirr/go123/exc"
)

// loadZdumpPy loads a zdump file and normalizes escaped strings to the way go
// would escape them.
func loadZdumpPy(t *testing.T, path string) string {
	dump, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// python quotes "\v" as "\x0b", go as "\v"; same for "\f", "\a", "\b".
	// XXX this is a bit hacky. We could compare quoted strings as decoded,
	// but this would need zdump format parser which could contain other
	// bugs.  Here we want to compare output ideally bit-to-bit but those
	// \v vs \x0b glitches prevents that to be done directly. So here we
	// are with this ugly hack:
	var pyNoBackLetter = []struct{ backNoLetterRe, backLetter string }{
		{`\\x07`, `\a`},
		{`\\x08`, `\b`},
		{`\\x0b`, `\v`},
		{`\\x0c`, `\f`},
	}

	for _, __ := range pyNoBackLetter {
		re := regexp.MustCompile(__.backNoLetterRe)
		dump = re.ReplaceAllLiteral(dump, []byte(__.backLetter))
	}

	return string(dump)
}

func withTestdataFs(t testing.TB, db string, f func(zstor zodb.IStorage)) {
	zstor, err := zodb.Open(context.Background(), fmt.Sprintf("../../zodb/storage/fs1/testdata/%s.fs", db), &zodb.OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}

	defer exc.XRun(zstor.Close)

	f(zstor)
}

func TestZodbDump(t *testing.T) {
	testv := []string{"1", "empty"}
	for _, tt := range testv {
		t.Run("db=" + tt, func(t *testing.T) {
			withTestdataFs(t, tt, func(zstor zodb.IStorage) {
				buf := bytes.Buffer{}

				err := Dump(context.Background(), &buf, zstor, 0, zodb.TidMax, false)
				if err != nil {
					t.Fatal(err)
				}

				dumpOk := loadZdumpPy(t, fmt.Sprintf("testdata/%s.zdump.pyok", tt))

				if dumpOk != buf.String() {
					t.Errorf("dump different:\n%v", diff.Diff(dumpOk, buf.String()))
				}
			})
		})
	}
}


func BenchmarkZodbDump(b *testing.B) {
	// FIXME small testdata/1.fs is not representative for benchmarking
	withTestdataFs(b, "1", func(zstor zodb.IStorage) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := Dump(context.Background(), ioutil.Discard, zstor, 0, zodb.TidMax, false)
			if err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
	})
}
