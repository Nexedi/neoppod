// TODO copyright/license

package main

//go:generate sh -c "python2 -m zodbtool.zodbdump testdata/1.conf >testdata/1.zdump.ok"

import (
	"bytes"
	"io/ioutil"
	"testing"

	"../../../storage/fs1"
	"../../../zodb"

	"lab.nexedi.com/kirr/go123/exc"
)

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

	dumpOk, err := ioutil.ReadFile("testdata/1.zdump.ok")
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(dumpOk, buf.Bytes()) {
		t.Errorf("dump different TODO show diff")	// XXX github.com/sergi/go-diff.git
	}
}
