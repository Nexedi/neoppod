// XXX license/copyright

package fs1

import (
	"sort"
	"testing"

	"../../zodb"
)

type indexEntry struct {
	oid zodb.Oid
	pos int64
}

type byOid []indexEntry

func (p byOid) Len() int           { return len(p) }
func (p byOid) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p byOid) Less(i, j int) bool { return p[i].oid < p[j].oid }

func TestIndexLookup(t *testing.T) {
	// the lookup is tested in cznic.b itself
	// here we only lightly exercise it
	fsi := fsIndexNew()

	if fsi.Len() != 0 {
		t.Errorf("index created non empty")
	}


	tt := [...]indexEntry {
		{0x0000000000000000, 111},
		{0x0000000000000001, 222},
		{0x000000000000ffff, 333},
		{0x0000000000001234, 444},
		{0x0000000000010002, 555},
		{0x0000000000010001, 665},
		{0xffffffffffffffff, 777},
		{0xfffffffffffffff0, 888},
		{0x8000000000000000, 999},
		{0xa000000000000000, 0x7fffffffffffffff},
	}

	// set
	for _, entry := range tt {
		fsi.Set(entry.oid, entry.pos)
	}

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


func TestIndexSaveLoad(t *testing.T) {
}
