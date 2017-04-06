// TODO copyright/license

package main

//go:generate sh -c "python2 -m zodbtools.zodb dump ../../../storage/fs1/testdata/1.fs >testdata/1.zdump.pyok"

import (
	"bytes"
	"io/ioutil"
	"regexp"
	"testing"

	"../../../storage/fs1"
	"../../../zodb"

	"github.com/sergi/go-diff/diffmatchpatch"
	"lab.nexedi.com/kirr/go123/exc"
)

// diff computes difference for two strings a and b
// XXX -> xtesting ?
// XXX dup in fstail_test.go
func diff(a, b string) string {
	dmp := diffmatchpatch.New()
	diffv := dmp.DiffMain(a, b, /*checklines=*/false)
	return dmp.DiffPrettyText(diffv)
}

// loadZdumpPy loads a zdump file and normalizes escaped strings to the way go
// would escape them
func loadZdumpPy(t *testing.T, path string) string {
	dump, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// python qoutes "\v" as "\x0b", go as "\v"; same for "\f", "\a", "\b".
	// XXX this is a bit hacky. We could compare quoted strings as decoded,
	// but this would need zdump format parser which could contain other
	// bugs.  Here we want to compare output ideally bit-to-bit but those
	// \v vs \x0b glitches prevents that to be done directly. So here we
	// are with this ugly hack:
	var pyNoBackLetter = []struct {backNoLetterRe, backLetter string} {
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

func withTestdata1Fs(t testing.TB, f func(fs *fs1.FileStorage)) {
	fs, err := fs1.Open("../../../storage/fs1/testdata/1.fs")	// XXX read-only, path?
	if err != nil {
		t.Fatal(err)
	}

	defer exc.XRun(fs.Close)

	f(fs)
}

func TestZodbDump(t *testing.T) {
	withTestdata1Fs(t, func(fs *fs1.FileStorage) {
		buf := bytes.Buffer{}

		err := zodbDump(&buf, fs, 0, zodb.TidMax, false)
		if err != nil {
			t.Fatal(err)
		}

		dumpOk := loadZdumpPy(t, "testdata/1.zdump.pyok")

		if dumpOk != buf.String() {
			t.Errorf("dump different:\n%v", diff(dumpOk, buf.String()))
		}
	})
}


func BenchmarkZodbDump(b *testing.B) {
	// FIXME small testdata/1.fs is not representative for benchmarking
	withTestdata1Fs(b, func(fs *fs1.FileStorage) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := zodbDump(ioutil.Discard, fs, 0, zodb.TidMax, false)
			if err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
	})
}
