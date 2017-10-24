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

package main

import (
	"bytes"
	"fmt"
	"go/build"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"lab.nexedi.com/kirr/go123/exc"
)

func xglob(t *testing.T, pattern string) []string {
	t.Helper()
	matchv, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	return matchv
}


type TreePrepareMode int
const (
	TreePrepareGolden TreePrepareMode = iota // prepare golden tree - how `gotrace gen` result should look like
	TreePrepareWork                          // prepare work tree - inital state for `gotrace gen` to run
)

// prepareTestTree copies files from src to dst recursively processing *.ok and *.rm depending on mode.
//
// dst should not initially exist
func prepareTestTree(src, dst string, mode TreePrepareMode) error {
	err := os.MkdirAll(dst, 0777)
	if err != nil {
		return err
	}

	return filepath.Walk(src, func(srcpath string, info os.FileInfo, err error) error {
		if srcpath == src /* skip root */ || err != nil {
			return err
		}

		dstpath := dst + strings.TrimPrefix(srcpath, src)
		if info.IsDir() {
			err := os.Mkdir(dstpath, 0777)
			return err
		}

		// NOTE since files are walked in lexical order <f>.ok or
		// <f>.rm is always guaranteed to go after <f>.

		var isOk, isRm bool
		if strings.HasSuffix(srcpath, ".ok") {
			isOk = true
			dstpath = strings.TrimSuffix(dstpath, ".ok")
		}
		if strings.HasSuffix(srcpath, ".rm") {
			isRm = true
			dstpath = strings.TrimSuffix(dstpath, ".rm")
		}

		data, err := ioutil.ReadFile(srcpath)
		if err != nil {
			return err
		}

		switch mode {
		case TreePrepareGolden:
			// ok files are written as is

			// no removed files
			if isRm {
				return nil
			}

		case TreePrepareWork:
			// no ok files initially
			if isOk {
				return nil
			}
			// files to remove - prepopulate with magic
			if isRm {
				data = []byte(magic)
			}
		}

		err = ioutil.WriteFile(dstpath, data, info.Mode())
		return err
	})
}

func xprepareTree(src, dst string, mode TreePrepareMode) {
	err := prepareTestTree(src, dst, mode)
	exc.Raiseif(err)
}

// diffR compares two directories recursively
func diffR(patha, pathb string) (diff string, err error) {
	cmd := exec.Command("diff", "-urN", patha, pathb)
	out, err := cmd.Output()
	if e, ok := err.(*exec.ExitError); ok {
		if e.Sys().(syscall.WaitStatus).ExitStatus() == 1 {
			err = nil // diff signals with 1 just a difference - problem exit code is 2
		} else {
			err = fmt.Errorf("diff %s %s:\n%s", patha, pathb, e.Stderr)
		}
	}

	return string(out), err
}

func TestGoTrace(t *testing.T) {
	tmp, err := ioutil.TempDir("", "t-gotrace")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	good := tmp + "/good"
	work := tmp + "/work"

	xprepareTree("testdata", good, TreePrepareGolden)
	xprepareTree("testdata", work, TreePrepareWork)

	// test build context with GOPATH set to work tree
	var tBuildCtx = &build.Context{
		GOARCH:     "amd64",
		GOOS:       "linux",
		GOROOT:     runtime.GOROOT(),
		GOPATH:     work,
		CgoEnabled: true,
		Compiler:   runtime.Compiler,
	}

	// XXX autodetect (go list ?)
	testv := []string{"a/pkg1", "b/pkg2", "c/pkg3", "d/pkg4"}

	for _, tpkg := range testv {
		// verify `gotrace gen`
		err = tracegen(tpkg, tBuildCtx, "" /* = local imorts disabled */)
		if err != nil {
			t.Errorf("%v: %v", tpkg, err)
		}

		diff, err := diffR(good+"/src/"+tpkg, work+"/src/"+tpkg)
		if err != nil {
			t.Fatalf("%v: %v", tpkg, err)
		}

		if diff != "" {
			t.Errorf("%v: gold & work differ:\n%s", tpkg, diff)
		}

		// verify `gotrace list`
		var tlistBuf bytes.Buffer
		err = tracelist(&tlistBuf, tpkg, tBuildCtx, "" /* = local imports disabled */)
		if err != nil {
			t.Fatalf("%v: %v", tpkg, err)
		}

		tlistOk, err := ioutil.ReadFile(work + "/src/" + tpkg + "/tracelist.txt")
		if err != nil {
			t.Fatalf("%v: %v", tpkg, err)
		}

		tlist := tlistBuf.Bytes()
		if !bytes.Equal(tlist, tlistOk) {
			t.Errorf("%v: tracelist differ:\nhave:\n%s\nwant:\n%s", tpkg, tlist, tlistOk)
		}
	}
}
