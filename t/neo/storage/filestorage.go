// XXX license

// filestorage support  XXX text
package storage

import (
	"os"
	"../zodb"
)

type FileStorage struct {
    f *os.File	// XXX naming -> file ?
}

// IStorage
var _ zodb.IStorage = (*FileStorage)(nil)

type TxnRecHead struct {
    Tid             zodb.Tid
    RecLenm8        uint64
    Status          zodb.TxnStatus
    //UserLen         uint16
    //DescriptionLen  uint16
    //ExtensionLen    uint16
    User            []byte  // TODO Encode ^^^
    Description     []byte
    Extension       []byte

    Datav   []DataRec
}

type DataRec struct {
    Oid             zodb.Oid
    Tid             zodb.Tid
    PrevDataRecPos  uint64  // previous-record file-position
    TxnPos          uint64  // beginning of transaction record file position
    // 2-bytes with zero values. (Was version length.)
    //DataLen     uint64
    Data            []byte
    DataRecPos      uint64  // if Data == nil -> byte position of data record containing data
}


func (rh *TxnRecHead) MarshalFS() []byte {
    panic("TODO")
}

func (rh *TxnRecHead) UnmarshalFS(data []byte) {
    //TODO
}



func NewFileStorage(path string) (*FileStorage, error) {
    f, err := os.Open(path)	// note opens in O_RDONLY
    if err != nil {
        return nil, err
    }
    // TODO read file header
    //Read(f, 4) != "FS21" -> invalid header
    return &FileStorage{f: f}, nil
}

func (f *FileStorage) Close() error {
    err := f.f.Close()
    if err != nil {
        return err
    }
    f.f = nil
    return nil
}

func (f *FileStorage) Iterate(start, stop zodb.Tid) zodb.IStorageIterator {
    if start != zodb.Tid0 || stop != zodb.TidMax {
        panic("TODO start/stop support")
    }

    // TODO
    return nil
}
