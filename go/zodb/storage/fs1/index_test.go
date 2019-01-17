// Copyright (C) 2017-2019  Nexedi SA and Contributors.
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

package fs1

//go:generate ./py/gen-testdata

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"

	"lab.nexedi.com/kirr/neo/go/internal/xtesting"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1/fsb"
)

type indexEntry struct {
	oid zodb.Oid
	pos int64
}

type byOid []indexEntry

func (p byOid) Len() int           { return len(p) }
func (p byOid) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p byOid) Less(i, j int) bool { return p[i].oid < p[j].oid }

var indexTest1 = [...]indexEntry{
	{0x0000000000000000, 111},
	{0x0000000000000001, 222},
	{0x000000000000ffff, 333},
	{0x0000000000001234, 444},
	{0x0000000000010002, 555},
	{0x0000000000010001, 665},
	{0xffffffffffffffff, 777},
	{0xfffffffffffffff0, 888},
	{0x8000000000000000, 999},
	{0xa000000000000000, 0x0000ffffffffffff},
}

func setIndex(fsi *Index, kv []indexEntry) {
	for _, entry := range kv {
		fsi.Set(entry.oid, entry.pos)
	}
}

// XXX unneded after Tree.Dump() was made to work ok
func treeString(t *fsb.Tree) string {
	entryv := []string{}

	e, _ := t.SeekFirst()
	if e != nil {
		defer e.Close()
		for {
			k, v, stop := e.Next()
			if stop != nil {
				break
			}
			entryv = append(entryv, fmt.Sprintf("%v: %v", k, v))
		}
	}

	return "{" + strings.Join(entryv, ", ") + "}"
}

func TestIndexLookup(t *testing.T) {
	// the lookup is tested in cznic.b itself
	// here we only lightly exercise it
	fsi := IndexNew()

	if fsi.Len() != 0 {
		t.Errorf("index created non empty")
	}


	tt := indexTest1

	// set
	setIndex(fsi, tt[:])

	// get
	for _, entry := range tt {
		pos, ok := fsi.Get(entry.oid)
		if !(pos == entry.pos && ok == true) {
			t.Errorf("fsi[%x] -> got (%x, %v)  ; want (%x, true)", entry.oid, pos, ok, entry.pos)
		}

		// try non-existing entries too
		oid := entry.oid ^ (1 << 32)
		pos, ok = fsi.Get(oid)
		if !(pos == 0 && ok == false) {
			t.Errorf("fsi[%x] -> got (%x, %v)  ; want (0, false)", oid, pos, ok)
		}
	}

	// iter
	e, err := fsi.SeekFirst()
	if err != nil {
		t.Fatal(err)
	}

	sort.Sort(byOid(tt[:]))

	i := 0
	for ; ; i++ {
		oid, pos, errStop := e.Next()
		if errStop != nil {
			break
		}


		entry := indexEntry{oid, pos}
		entryOk := tt[i]
		if entry != entryOk {
			t.Errorf("iter step %d: got %v  ; want %v", i, entry, entryOk)
		}
	}

	if i != len(tt) {
		t.Errorf("iter ended at step %v  ; want %v", i, len(tt))
	}
}

func checkIndexEqual(t *testing.T, subject string, fsi1, fsi2 *Index) {
	if fsi1.Equal(fsi2) {
		return
	}

	if fsi1.TopPos != fsi2.TopPos {
		t.Errorf("%s: topPos mismatch: %v  ; want %v", subject, fsi1.TopPos, fsi2.TopPos)
	}

	if !treeEqual(fsi1.Tree, fsi2.Tree) {
		t.Errorf("%s: trees mismatch:\nhave: %v\nwant: %v", subject, fsi1.Tree.Dump(), fsi2.Tree.Dump())
		//t.Errorf("index load: trees mismatch:\nhave: %v\nwant: %v", treeString(fsi2.Tree), treeString(fsi.Tree))
	}

}

func TestIndexSaveLoad(t *testing.T) {
	workdir := xworkdir(t)

	fsi := IndexNew()
	fsi.TopPos = int64(786)
	setIndex(fsi, indexTest1[:])

	err := fsi.SaveFile(workdir + "/1.fs.index")
	if err != nil {
		t.Fatal(err)
	}

	fsi2, err := LoadIndexFile(workdir + "/1.fs.index")
	if err != nil {
		t.Fatal(err)
	}

	checkIndexEqual(t, "index load", fsi2, fsi)

	// TODO check with
	// {0xb000000000000000, 0x7fffffffffffffff}, // will cause 'entry position too large'
}

var _1fs_index = func() *Index {
	idx := IndexNew()
	idx.TopPos = _1fs_indexTopPos
	setIndex(idx, _1fs_indexEntryv[:])
	return idx
}()

// test that we can correctly load index data as saved by zodb/py
func TestIndexLoadFromPy(t *testing.T) {
	fsiPy, err := LoadIndexFile("testdata/1.fs.index")
	if err != nil {
		t.Fatal(err)
	}

	checkIndexEqual(t, "index load", fsiPy, _1fs_index)
}

// test zodb/py can read index data as saved by us
func TestIndexSaveToPy(t *testing.T) {
	xtesting.NeedPy(t, "ZODB")
	workdir := xworkdir(t)

	err := _1fs_index.SaveFile(workdir + "/1.fs.index")
	if err != nil {
		t.Fatal(err)
	}

	// now ask python part to compare testdata and saved-by-us index
	cmd := exec.Command("./py/indexcmp", "testdata/1.fs.index", workdir+"/1.fs.index")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		t.Fatalf("zodb/py read/compare index: %v", err)
	}
}

func TestIndexBuildVerify(t *testing.T) {
	index, err := BuildIndexForFile(context.Background(), "testdata/1.fs", nil)
	if err != nil {
		t.Fatalf("index build: %v", err)
	}

	if !index.Equal(_1fs_index) {
		t.Fatal("computed index differ from expected")
	}

	_, err = index.VerifyForFile(context.Background(), "testdata/1.fs", -1, nil)
	if err != nil {
		t.Fatalf("index verify: %v", err)
	}

	pos0, _ := index.Get(0)
	index.Set(0, pos0+1)
	_, err = index.VerifyForFile(context.Background(), "testdata/1.fs", -1, nil)
	if err == nil {
		t.Fatalf("index verify: expected error after tweak")
	}
}


func BenchmarkIndexLoad(b *testing.B) {
	// FIXME small testdata/1.fs is not representative for benchmarks
	for i := 0; i < b.N; i++ {
		_, err := LoadIndexFile("testdata/1.fs.index")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexSave(b *testing.B) {
	// FIXME small testdata/1.fs is not representative for benchmarks
	index, err := LoadIndexFile("testdata/1.fs.index")
	if err != nil {
		b.Fatal(err)
	}

	workdir := xworkdir(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = index.SaveFile(workdir + "/1.fs.index")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexGet(b *testing.B) {
	// FIXME small testdata/1.fs is not representative for benchmarks
	fsi, err := LoadIndexFile("testdata/1.fs.index")
	if err != nil {
		b.Fatal(err)
	}

	oid := zodb.Oid(1)
	//oid := zodb.Oid(0x000000000000ea65)
	//v, _ := fsi.Get(oid)
	//fmt.Println(oid, v)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fsi.Get(oid)
	}
}

var workRoot string

func TestMain(m *testing.M) {
	// setup work root for all tests
	workRoot, err := ioutil.TempDir("", "t-index")
	if err != nil {
		log.Fatal(err)
	}

	exit := m.Run()

	os.RemoveAll(workRoot)

	os.Exit(exit)
}

// create temp dir inside workRoot
func xworkdir(t testing.TB) string {
	work, err := ioutil.TempDir(workRoot, "")
	if err != nil {
		t.Fatal(err)
	}
	return work
}
