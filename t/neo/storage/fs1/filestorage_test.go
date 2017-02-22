// XXX license/copyright

package fs1

import (
	"bytes"
	"strconv"
	"testing"

	"../../zodb"
)

// one entry inside transaction
type txnEntry struct {
	header	DataHeader
	data	[]byte
}

type dbEntry struct {
	header	TxnHeader
	entryv	[]txnEntry
}

func TestLoad(t *testing.T) {
	fs, err := NewFileStorage("testdata/1.fs")
	if err != nil {
		t.Fatal(err)
	}

	for _, dbe := range _1fs_dbEntryv {
		for _, txe := range dbe.entryv {
			txh := txe.header
			if txh.DataLen == 0 {
				continue	// FIXME skipping backpointers
			}

			xid := zodb.Xid{zodb.XTid{txh.Tid, false}, txh.Oid}	// loadSerial
			data, tid, err := fs.Load(xid)
			if err != nil {
				t.Errorf("load %v: %v", xid, err)
			}
			if tid != txh.Tid {
				t.Errorf("load %v: returned tid unexpected: %v", xid)
			}
			if !bytes.Equal(data, txe.data) {
				t.Errorf("load %v: different data:\nhave: %s\nwant: %s", xid, strconv.Quote(string(data)), strconv.Quote(string(txe.data)))
			}
		}
	}
}
