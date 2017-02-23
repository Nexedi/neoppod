// XXX license/copyright

package fs1

import (
	"bytes"
	"testing"

	"../../zodb"
)

// one database transaction record
type dbEntry struct {
	Header	TxnHeader
	Entryv	[]txnEntry
}

// one entry inside transaction
type txnEntry struct {
	Header	 DataHeader
	rawData	 []byte		// what is on disk, e.g. it can be backpointer
	userData []byte		// data client should see on load; nil means same as RawData
}

// Data returns data a client should see
func (txe *txnEntry) Data() []byte {
	data := txe.userData
	if data == nil {
		data = txe.rawData
	}
	return data
}

// successfull result of load for an oid
type oidLoadedOk struct {
	tid	zodb.Tid
	data	[]byte
}

// current knowledge of what was "before" for an oid as we scan over
// data base entries
type beforeMap map[zodb.Oid]oidLoadedOk

func checkLoad(t *testing.T, fs *FileStorage, xid zodb.Xid, expect oidLoadedOk) {
	data, tid, err := fs.Load(xid)
	if err != nil {
		t.Errorf("load %v: %v", xid, err)
	}
	if tid != expect.tid {
		t.Errorf("load %v: returned tid unexpected: %v  ; want: %v", xid, tid, expect.tid)
	}
	if !bytes.Equal(data, expect.data) {
		t.Errorf("load %v: different data:\nhave: %q\nwant: %q", xid, data, expect.data)
	}
}

func TestLoad(t *testing.T) {
	fs, err := NewFileStorage("testdata/1.fs")
	if err != nil {
		t.Fatal(err)
	}

	before := beforeMap{}

	for _, dbe := range _1fs_dbEntryv {
		for _, txe := range dbe.Entryv {
			txh := txe.Header

			// loadSerial
			xid := zodb.Xid{zodb.XTid{txh.Tid, false}, txh.Oid}
			checkLoad(t, fs, xid, oidLoadedOk{txh.Tid, txe.Data()})

			// loadBefore
			xid = zodb.Xid{zodb.XTid{txh.Tid, true}, txh.Oid}
			expect, ok := before[txh.Oid]
			// TODO also test for getting error when !ok
			if ok {
				checkLoad(t, fs, xid, expect)
			}

			before[txh.Oid] = oidLoadedOk{txh.Tid, txe.Data()}

		}
	}

	// loadBefore with TidMax
	for oid, expect := range before {
		xid := zodb.Xid{zodb.XTid{zodb.TidMax, true}, oid}
		checkLoad(t, fs, xid, expect)
	}
}
