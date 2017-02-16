// XXX license/copyright

package fs1

//go:generate ./gen-testdata

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"

	"../../zodb"
	"./fsb"
)

type indexEntry struct {
	oid zodb.Oid
	pos int64
}

type byOid []indexEntry

func (p byOid) Len() int           { return len(p) }
func (p byOid) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p byOid) Less(i, j int) bool { return p[i].oid < p[j].oid }

var indexTest1 = [...]indexEntry {
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

func setIndex(fsi *fsIndex, kv []indexEntry) {
	for _, entry := range kv {
		fsi.Set(entry.oid, entry.pos)
	}
}

// test whether two trees are equal
func treeEqual(a, b *fsb.Tree) bool {
	if a.Len() != b.Len() {
		return false
	}

	ea, _ := a.SeekFirst()
	eb, _ := b.SeekFirst()

	if ea == nil {
		// this means len(a) == 0 -> len(b) == 0 -> eb = nil
		return true
	}

	defer ea.Close()
	defer eb.Close()

	for {
		ka, va, stopa := ea.Next()
		kb, vb, stopb := eb.Next()

		if stopa != nil || stopb != nil {
			if stopa != stopb {
				panic("same-length trees iteration did not end at the same time")
			}
			break
		}

		if !(ka == kb && va == vb) {
			return false
		}
	}

	return true
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
	fsi := fsIndexNew()

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
		oid := entry.oid ^ (1<<32)
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
	for ;; i++ {
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

func checkIndexEqual(t *testing.T, subject string, topPos1, topPos2 int64, fsi1, fsi2 *fsIndex) {
	if topPos1 != topPos2 {
		t.Errorf("%s: topPos mismatch: %v  ; want %v", subject, topPos1, topPos2)
	}

	if !treeEqual(fsi1.Tree, fsi2.Tree) {
		t.Errorf("%s: trees mismatch:\nhave: %v\nwant: %v", subject, fsi1.Tree.Dump(), fsi2.Tree.Dump())
		//t.Errorf("index load: trees mismatch:\nhave: %v\nwant: %v", treeString(fsi2.Tree), treeString(fsi.Tree))
	}

}

func TestIndexSaveLoad(t *testing.T) {
	workdir := xworkdir(t)

	topPos := int64(786)
	fsi := fsIndexNew()
	setIndex(fsi, indexTest1[:])

	err := fsi.SaveFile(topPos, workdir + "/1.fs.index")
	if err != nil {
		t.Fatal(err)
	}

	topPos2, fsi2, err := LoadIndexFile(workdir + "/1.fs.index")
	if err != nil {
		t.Fatal(err)
	}

	checkIndexEqual(t, "index load", topPos2, topPos, fsi2, fsi)

	// TODO check with
	// {0xb000000000000000, 0x7fffffffffffffff}, // will cause 'entry position too large'
}


// test that we can correctly load index data as saved by zodb/py
func TestIndexLoadFromPy(t *testing.T) {
	topPosPy, fsiPy, err := LoadIndexFile("testdata/1.fs.index")
	if err != nil {
		t.Fatal(err)
	}

	fsiExpect := fsIndexNew()
	setIndex(fsiExpect, _1fs_indexEntryv[:])

	checkIndexEqual(t, "index load", topPosPy, _1fs_indexTopPos, fsiPy, fsiExpect)
}

// test zodb/py can read index data as saved by us
func TestIndexSaveToPy(t *testing.T) {
	needZODBPy(t)
	workdir := xworkdir(t)

	fsi := fsIndexNew()
	setIndex(fsi, _1fs_indexEntryv[:])

	err := fsi.SaveFile(_1fs_indexTopPos, workdir + "/1.fs.index")
	if err != nil {
		t.Fatal(err)
	}

	// now ask python part to compare testdata and save-by-us index
	cmd := exec.Command("./indexcmp", "testdata/1.fs.index", workdir + "/1.fs.index")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		t.Fatalf("zodb/py read/compare index: %v", err)
	}
}

var haveZODBPy = false
var workRoot string

func TestMain(m *testing.M) {
	// check whether we have zodb/py
	cmd := exec.Command("python2", "-c", "import ZODB")
	err := cmd.Run()
	if err == nil {
		haveZODBPy = true
	}

	// setup work root for all tests
	workRoot, err = ioutil.TempDir("", "t-index")
	if err != nil {
		log.Fatal(err)
	}

	exit := m.Run()

	os.RemoveAll(workRoot)

	os.Exit(exit)
}

func needZODBPy(t *testing.T) {
	if haveZODBPy {
		return
	}
	t.Skipf("zodb/py is not available")
}

// create temp dir inside workRoot
func xworkdir(t *testing.T) string {
	work, err := ioutil.TempDir(workRoot, "")
	if err != nil {
		t.Fatal(err)
	}
	return work
}
