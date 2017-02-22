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
	Header	DataHeader
	rawData	[]byte		// what is on disk, e.g. it can be backpointer
	data	[]byte		// data client should see on load; nil means same as RawData
}

func (txe *txnEntry) Data() []byte {
	data := txe.data
	if data == nil {
		data = txe.rawData
	}
	return data
}

type dbEntry struct {
	Header	TxnHeader
	Entryv	[]txnEntry
}

func TestLoad(t *testing.T) {
	fs, err := NewFileStorage("testdata/1.fs")
	if err != nil {
		t.Fatal(err)
	}

	for _, dbe := range _1fs_dbEntryv {
		for _, txe := range dbe.Entryv {
			txh := txe.Header

			xid := zodb.Xid{zodb.XTid{txh.Tid, false}, txh.Oid}	// loadSerial
			data, tid, err := fs.Load(xid)
			if err != nil {
				t.Errorf("load %v: %v", xid, err)
			}
			if tid != txh.Tid {
				t.Errorf("load %v: returned tid unexpected: %v", xid)
			}
			if !bytes.Equal(data, txe.Data()) {
				t.Errorf("load %v: different data:\nhave: %s\nwant: %s", xid, strconv.Quote(string(data)), strconv.Quote(string(txe.Data())))
			}
		}
	}
}
