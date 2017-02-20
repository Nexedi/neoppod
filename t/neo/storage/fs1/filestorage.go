// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 2, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
//
// XXX based on code from ZODB ?

// FileStorage v1.  XXX text
package fs1

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"../../zodb"
)

type FileStorage struct {
	f		*os.File	// XXX naming -> file ?
	index	*fsIndex
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

	//Datav   []DataRec
}

// DataHeader represents header of a data record
type DataHeader struct {
	Oid             zodb.Oid
	Tid             zodb.Tid
	PrevDataRecPos  uint64  // previous-record file-position
	TxnPos          uint64  // position of transaction record this data record belongs to
	_		uint16	// 2-bytes with zero values. (Was version length.)
	DataLen		uint64	// length of following data. if 0 -> following = 8 bytes backpointer
				// if backpointer == 0 -> oid deleted
	//Data            []byte
	//DataRecPos      uint64  // if Data == nil -> byte position of data record containing data
}

const DataHeaderSize	= 42

// ErrDataRead is returned on data record read / decode errors
type ErrDataRead struct {
	Pos	int64
	Err	error
}

func (e *ErrDataRead) Error() string {
	return fmt.Sprintf("data read: record @%v: %v", e.Pos, e.Err)
}

// XXX -> zodb?
var ErrVersionNonZero = errors.New("non-zero version")



// XXX io.ReaderAt -> *os.File  (if iface conv costly)
func (dh *DataHeader) decode(r io.ReaderAt, pos int64, tmpBuf *[DataHeaderSize]byte) error {
	n, err := r.ReadAt(tmpBuf[:], pos)
	if n == DataHeaderSize {
		err = nil	// we don't care if it was EOF after full header read	XXX ok?
	}

	if err != nil {
		return &ErrDataRead{pos, err}
	}

	dh.Oid.Decode(tmpBuf[0:])
	dh.Tid.Decode(tmpBuf[8:])
	dh.PrevDataRecPos = binary.BigEndian.Uint64(tmpBuf[16:])
	dh.TxnPos = binary.BigEndian.Uint64(tmpBuf[24:])
	verlen := binary.BigEndian.Uint16(tmpBuf[32:])
	dh.DataLen = binary.BigEndian.Uint64(tmpBuf[34:])

	if verlen != 0 {
		return &ErrDataRead{pos, ErrVersionNonZero}
	}

	return nil
}

func (dh *DataHeader) Decode(r io.ReaderAt, pos int64) error {
	var tmpBuf [DataHeaderSize]byte
	return dh.decode(r, pos, &tmpBuf)
}



func NewFileStorage(path string) (*FileStorage, error) {
	f, err := os.Open(path)	// XXX opens in O_RDONLY
	if err != nil {
		return nil, err	// XXX err more context ?
	}
	// TODO read file header
	//Read(f, 4) != "FS21" -> invalid header
	return &FileStorage{f: f}, nil

	// TODO read/recreate index
}

func (fs *FileStorage) LoadBefore(oid zodb.Oid, beforeTid zodb.Tid) (data []byte, tid zodb.Tid, err error) {
	// lookup in index position of oid data record with latest transaction who changed this oid
	dataPos, ok := fs.index.Get(oid)
	if !ok {
		return nil, zodb.Tid(0), zodb.ErrOidMissing{Oid: oid}
	}

	// search backwards for when we first have data record with tid < beforeTid
}

func (fs *FileStorage) Close() error {
	// TODO dump index
	err := fs.f.Close()
	if err != nil {
		return err
	}
	fs.f = nil
	return nil
}

func (fs *FileStorage) StorageName() string {
	return "FileStorage v1"
}

func (fs *FileStorage) Iterate(start, stop zodb.Tid) zodb.IStorageIterator {
	if start != zodb.Tid0 || stop != zodb.TidMax {
		panic("TODO start/stop support")
	}

	// TODO
	return nil
}
