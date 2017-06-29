package main

import (
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
	TreePrepareWork				 // prepare work tree - inital state for `gotrace gen` to run
)

// prepareTestTree copies files from src to dst recursively processing *.ok and *.rm depending on mode
// dst should not initially exist
func prepareTestTree(src, dst string, mode TreePrepareMode) error {
	println("AAA", dst)
	err := os.MkdirAll(dst, 0777)
	if err != nil {
		return err
	}

	return filepath.Walk(src, func(srcpath string, info os.FileInfo, err error) error {
		println("*", srcpath)
		if srcpath == src /* skip root */ || err != nil {
			return err
		}

		dstpath := dst + strings.TrimPrefix(srcpath, src)
		//println("·", dstpath)
		if info.IsDir() {
			err := os.Mkdir(dstpath, 0777)
			return err
		}

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

func TestGoTraceGen(t *testing.T) {
	tmp, err := ioutil.TempDir("", "t-gotrace")
	if err != nil {
		t.Fatal(err)
	}
	//defer os.RemoveAll(tmp)

	good := tmp + "/good"
	work := tmp + "/work"

	xprepareTree("testdata", good, TreePrepareGolden)
	xprepareTree("testdata", work, TreePrepareWork)

	// test build context with GOPATH set to work tree
	var tBuildCtx = &build.Context{
				GOARCH: "amd64",
				GOOS:	"linux",
				GOROOT:	runtime.GOROOT(),
				GOPATH:	work,
				Compiler: runtime.Compiler,
	}

	// XXX autodetect (go list ?)
	testv := []string{"a/pkg1", "b/pkg2"}

	for _, tpkg := range testv {
		err = tracegen(tpkg, tBuildCtx)
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
	}
}
