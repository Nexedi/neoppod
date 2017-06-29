package main

import (
	"go/build"
	"testing"
)

/*
// memWriter saves in memory filesystem modifications gotrace wanted to made
type memWriter struct {
	wrote	map[string]string	// path -> data
	removed StrSet
}

func (mw *memWriter) writeFile(path string, data []byte) error {
	mw.wrote[path] = string(data)
	return nil
}

func (mw *memWriter) removeFile(path) error {
	mw.removed.Add(path)
	return nil
}
*/

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
func prepareTestTree(src, dst string, mode prepareMode) error {
	err := os.MkdirAll(dst, 0777)
	if err != nil {
		return err
	}

	filepath.Walk(src, func(srcpath string, info os.FileInfo, err error) error {
		dstpath := dst + "/" + strings.TrimPrefix(srcpath, src)
		if info.IsDir() {
			err := os.Mkdir(dstpath, 0777)
			if err != nil {
				return err
			}
			return cpR(srcpath, dstpath)
		}

		var isOk, isRm bool
		if strings.HasSuffix(srcpath, ".ok") {
			isOk = true
			dstpath = strings.TrimSuffix(dstpath, ".ok")
		}
		if strings.hasSuffix(srcpath, ".rm") {
			isRm = true
			dstpath = strings.TrimSuffix(dstpath, ".rm")
		}

		data, err := ioutil.ReadFile(srcpath)
		if err != nil {
			return err
		}

		switch mode {
		case prepareGolden:
			// no removed files in golden tree
			if isRm {
				return nil
			}

		case prepareWork:
			// no ok files initially in work tree
			if isOk {
				return nil
			}
			// files to remove - prepopulate with magic
			if isRm {
				data = []byte(magic)
			}
		}

		err = ioutil.WriteFile(dstpath, data, info.Mode())
		if err != nil {
			return err
		}

		return nil
	})
}

xprepareTree(t *testing.T, src, dst string, mode prepareMode) {
	err := prepareTestTree(src, dst)
	exc.Raiseif(err)
}

func TestGoTraceGen(t *testing.T) {
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
	var tBuildCtx = build.Context{
				GOARCH: "amd64",
				GOOS:	"linux",
				GOROOT:	runtime.GOROOT(),
				GOPATH:	work,
				Compiler: runtime.Compiler(),
	}

	// XXX autodetect (go list ?)
	testv := []string{"a/pkg1", "b/pkg2"}

	for _, tpkg := range testv {
		err = tracegen(tpkg, tBuildCtx)
		if err != nil {
			t.Errorf("%v: %v", tpkg, err)
		}

		err := diffR(good+"/"+tpkg, work+"/"+tpkg)
		if err != nil {
			t.Errorf("%v: %v", tpkg, err)
		}
	}
}
