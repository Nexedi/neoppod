// TODO copyright/license

package main

//go:generate sh -c "python2 -m ZODB.scripts.fstail -n 1000000 ../../testdata/1.fs >testdata/1.fsdump.ok"

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

func TestFsTail(t *testing.T) {
	buf := bytes.Buffer{}

	err := fsTail(&buf, "../../testdata/1.fs", 1000000)
	if err != nil {
		t.Fatal(err)
	}

	dumpOk := loadFile(t, "testdata/1.fsdump.ok")

	if dumpOk != buf.String() {
		t.Errorf("dump different:\n%v", diff(dumpOk, buf.String()))
	}
}

func BenchmarkFsTail(b *testing.B) {
	// FIXME small testdata/1.fs is not representative for benchmarking
	for i := 0; i < b.N; i++ {
		err := fsTail(ioutil.Discard, "../../testdata/1.fs", 1000000)
		if err != nil {
			b.Fatal(err)
		}
	}
}
