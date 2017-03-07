// TODO copyright/license

package main

//go:generate sh -c "python2 -m zodbtool.zodbdump testdata/1.conf >testdata/1.zdump.ok"

import (
	"bytes"
	"io/ioutil"
	"testing"

	"../../../storage/fs1"
	"../../../zodb"

	"github.com/sergi/go-diff/diffmatchpatch"
	"lab.nexedi.com/kirr/go123/exc"
)

func diff(a, b string) string {
	dmp := diffmatchpatch.New()
	diffv := dmp.DiffMain(a, b, /*checklines=*/false)
	return dmp.DiffPrettyText(diffv)
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

	__, err := ioutil.ReadFile("testdata/1.zdump.ok")
	if err != nil {
		t.Fatal(err)
	}
	dumpOk := string(__)

	if dumpOk != buf.String() {
		t.Errorf("dump different:\n%v", diff(dumpOk, buf.String()))
	}
}
