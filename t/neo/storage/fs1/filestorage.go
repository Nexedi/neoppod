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
// XXX partly based on code from ZODB ?

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
	f	*os.File	// XXX naming -> file ?
	index	*fsIndex
}

// IStorage
var _ zodb.IStorage = (*FileStorage)(nil)


// XXX -> TxnHeader
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

	// XXX include word0 ?
}

const DataHeaderSize	= 42

// ErrData is returned on data record read / decode errors
type ErrDataRecord struct {
	Pos	int64	// position of data record
	Subj	string	// about what .Err is
	Err	error	// actual error
}

func (e *ErrDataRecord) Error() string {
	return fmr.Sprintf("data record @%v: %v: %v", e.Pos, e.Subj, e.Err)
}

// XXX -> zodb?
var ErrVersionNonZero = errors.New("non-zero version")



// decode reads and decodes data record header from a readerAt
// XXX io.ReaderAt -> *os.File  (if iface conv costly)
func (dh *DataHeader) decode(r io.ReaderAt, pos int64, tmpBuf *[DataHeaderSize]byte) error {
	n, err := r.ReadAt(tmpBuf[:], pos)
	if n == DataHeaderSize {
		err = nil // we don't mind if it was EOF after full header read
	}

	if err != nil {
		return &ErrDataRecord{pos, "read", err}
	}

	dh.Oid.Decode(tmpBuf[0:])
	dh.Tid.Decode(tmpBuf[8:])
	dh.PrevDataRecPos = binary.BigEndian.Uint64(tmpBuf[16:])
	dh.TxnPos = binary.BigEndian.Uint64(tmpBuf[24:])
	verlen := binary.BigEndian.Uint16(tmpBuf[32:])
	dh.DataLen = binary.BigEndian.Uint64(tmpBuf[34:])

	if verlen != 0 {
		return &ErrDataRecord{pos, "invalid header", ErrVersionNonZero}
	}

	return nil
}

// XXX do we need Decode when decode() is there?
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

// ErrOidLoad is returned when there is an error while loading oid
type ErrOidLoad struct {
	Oid	zodb.Oid
	Err	error
}

func (e *ErrOidLoad) Error() string {
	// TODO include whole (=|<)tid:oid ?
	return fmt.Sprintf("loading oid %v: %v", e.Oid, e.Err)
}

func (fs *FileStorage) Load(xid zodb.Xid) (data []byte, tid zodb.Tid, err error) {
	// lookup in index position of oid data record within latest transaction who changed this oid
	dataPos, ok := fs.index.Get(oid)
	if !ok {
		// XXX drop oid from ErrOidMissing ?
		return nil, zodb.Tid(0), &ErrOidLoad{oid, zodb.ErrOidMissing{Oid: oid}}
	}

	dh := DataHeader{Tid: zodb.TidMax}

	// search backwards for when we first have data record with tid < beforeTid
	for {
		prevTid := dh.Tid
		err = dh.Decode(fs.f, dataPos)
		if err != nil {
			return nil, zodb.Tid(0), &ErrOidLoad{oid, err}
		}

		// check data record consistency
		if dh.Oid != oid {
			// ... header invalid:
			return nil, zodb.Tid(0), &ErrOidLoad{oid, &ErrDataRecord{dataPos, "consistency check", "TODO unexpected oid")}
		}

		if dh.Tid >= prevTid { ... }
		if dh.TxnPos >= dataPos - TxnHeaderSize { ... }
		if dh.PrevDataRecPos >= dh.TxnPos - DataHeaderSize - 8 /* XXX */ { ... }

		if dh.Tid < beforeTid {
			break
		}

		// continue search
		dataPos = dh.PrevDataRecPos
		dataPos == 0 {
			// no such oid revision
			return nil, zodb.Tid(0), &ErrOidLoad{oid, zodb.ErrOidRevMissing{oid, "<", beforeTid}}
		}
	}

	// found dh.Tid < beforeTid
	// now read actual data / scan via backpointers
	if dh.DataLen == 0 {
		// backpointer - TODO
	}

	data = make([]byte, dh.DataLen)	// TODO -> slab ?
	n, err := fs.f.ReadAt(data, dataPos + DataHeaderSize)
	if n == len(data) {
		err = nil	// we don't mind to get EOF after full data read   XXX ok?
	}
	if err != nil {
		return nil, zodb.Tid(0), &ErrOidLoad{oid, err}
	}

	return data, dh.Tid, nil
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
