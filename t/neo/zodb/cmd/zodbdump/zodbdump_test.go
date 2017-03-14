// TODO copyright/license

package main

//go:generate sh -c "python2 -m zodbtool.zodbdump testdata/1.conf >testdata/1.zdump.pyok"

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

	// python qoutes "\v" as "\x0b", go as "\v"; same for "\f" vs "\x0c"
	// XXX this is a bit hacky. We could compare quoted strings as decoded,
	// but this would need zdump format parser which could contain other
	// bugs.  Here we want to compare output ideally bit-to-bit but those
	// \v vs \x0b glitches prevents that to be done directly. So here we
	// are with this ugly hack:
	r0b := regexp.MustCompile(`\\x0b`)
	r0c := regexp.MustCompile(`\\x0c`)
	dump = r0b.ReplaceAllLiteral(dump, []byte(`\v`))
	dump = r0c.ReplaceAllLiteral(dump, []byte(`\f`))
	return string(dump)
}


func TestZodbDump(t *testing.T) {
	buf := bytes.Buffer{}
	fs, err := fs1.Open("../../../storage/fs1/testdata/1.fs")	// XXX read-only, path?
	if err != nil {
		t.Fatal(err)
	}

	defer exc.XRun(fs.Close)

	err = zodbDump(&buf, fs, 0, zodb.TidMax, false)
	if err != nil {
		t.Fatal(err)
	}

	dumpOk := loadZdumpPy(t, "testdata/1.zdump.pyok")

	if dumpOk != buf.String() {
		t.Errorf("dump different:\n%v", diff(dumpOk, buf.String()))
	}
}
