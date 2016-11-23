// XXX license

// filestorage support  XXX text
package storage

type FileStorage struct {
    fd int
}

// IStorage
var _ IStorage = (*FileStorage)(nil)

type TxnRecHead struct {
    Tid             Tid
    RecLenm8        uint64
    Status          TxnStatus
    //UserLen         uint16
    //DescriptionLen  uint16
    //ExtensionLen    uint16
    User            []byte  // TODO Encode ^^^
    Description     []byte
    Extension       []byte

    Datav   []DataRec
}

type DataRec struct {
    Oid             Oid
    Tid             Tid
    PrevDataRecPos  uint64  // previous-record file-position
    TxnPos          uint64  // beginning of transaction record file position
    // 2-bytes with zero values. (Was version length.)
    //DataLen     uint64
    Data            []byte
    DataRecPos      uint64  // if Data == nil -> byte position of data record containing data
}


func (TxnRecHead *rh) MarshalFS() []byte {
    panic("TODO")
}

func (TxnRecHead *rh) UnmarshalFS(data []byte) {
    TODO
}



func NewFileStorage(path string) (*FileStorage, error) {
    fd, err := ...Open(path, O_RDONLY)
    if err != nil {
        return nil, err
    }
    // TODO read file header
    Read(fd, 4) != "FS21" -> invalid header
    return &FileStorage{fd: fd}
}

func (f *FileStorage) Close() error {
    err := Os.Close(f.fd)
    if err != nil {
        return err
    }
    f.fd = -1
    return nil
}

func (f *FileStorage) Iterate(start, stop Tid) IStorageIterator {
    if start != TID0 || stop != TIDMAX {
        panic("TODO start/stop support")
    }


}
