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
	topPos	int64		// position pointing just past last committed transaction
				// (= size(f) when no commit is in progress)

	// min/max tids committed
	tidMin, tidMax	zodb.Tid
}

// IStorage
var _ zodb.IStorage = (*FileStorage)(nil)


// TxnHeader represents header of a transaction record
type TxnHeader struct {
	// XXX -> TxnPos, TxnLen, PrevTxnLen ?
	Pos	int64	// position of transaction start
	PrevLen	int64	// whole previous transaction record length
			// (0 if there is no previous txn record)
	Len	int64	// whole transaction record length

	// XXX recheck ^^^ Len are validated on load

	// transaction metadata tself
	zodb.TxnInfo

	// underlying storage for user/desc/extension
	stringsMem	[]byte
}

const (
	txnHeaderFixSize	= 8+8+1+2+2+2		// = 23
	txnXHeaderFixSize	= 8 + txnHeaderFixSize	// with trail lengthm8 from previous record
)

// ErrTxnRecord is returned on transaction record read / decode errors
// XXX merge with ErrDataRecord -> ErrRecord{pos, "transaction|data", "read", err} ?
type ErrTxnRecord struct {
	Pos	int64	// position of transaction record
	Subj	string	// about what .Err is
	Err	error	// actual error
}

func (e *ErrTxnRecord) Error() string {
	return fmt.Sprintf("transaction record @%v: %v: %v", e.Pos, e.Subj, e.Err)
}

// DataHeader represents header of a data record
type DataHeader struct {
	Oid             zodb.Oid
	Tid             zodb.Tid
	PrevDataRecPos  int64	// previous-record file-position	XXX name
	TxnPos          int64	// position of transaction record this data record belongs to
	//_		uint16	// 2-bytes with zero values. (Was version length.)
	DataLen		uint64	// length of following data. if 0 -> following = 8 bytes backpointer	XXX -> int64 too ?
				// if backpointer == 0 -> oid deleted
	//Data            []byte
	//DataRecPos      uint64  // if Data == nil -> byte position of data record containing data

	// XXX include word0 ?
}

const dataHeaderSize	= 8+8+8+8+2+8	// = 42

// ErrDataRecord is returned on data record read / decode errors
type ErrDataRecord struct {
	Pos	int64	// position of data record
	Subj	string	// about what .Err is
	Err	error	// actual error
}

func (e *ErrDataRecord) Error() string {
	return fmt.Sprintf("data record @%v: %v: %v", e.Pos, e.Subj, e.Err)
}

// XXX -> zodb?
var ErrVersionNonZero = errors.New("non-zero version")


// load reads and decodes transaction record header from a readerAt
// pos: points to header begin	XXX text
// no requirements are made to previous th state	XXX text
// XXX io.ReaderAt -> *os.File  (if iface conv costly)
//func (th *TxnHeader) load(r io.ReaderAt, pos int64, tmpBuf *[txnXHeaderFixSize]byte) (n int, err error) {
func (th *TxnHeader) load(r io.ReaderAt, pos int64, tmpBuf *[txnXHeaderFixSize]byte) error {
	th.Pos = pos

	if pos > 4 + 8 {	// XXX -> magic
		// read together with previous's txn record redundand length
		n, err = r.ReadAt(tmpBuf[:], pos - 8)
		n -= 8	// relative to pos
		if n >= 0 {
			th.PrevLenm8 = uint64(binary.BigEndian.Uint64(tmpBuf[8-8:]))
			if th.PrevLenm8 < txnHeaderFixSize {
				panic("too small txn prev record length")	// XXX
			}
		}
	} else {
		th.PrevLenm8 = 0
		n, err = r.ReadAt(tmpBuf[8:], pos)
	}


	if err != nil {
		if err == io.EOF {
			if n == 0 {
				return err // end of stream
			}

			// EOF after txn header is not good
			err = io.ErrUnexpectedEOF
		}

		return n, &ErrTxnRecord{pos, "read", err}
	}

	th.Tid = zodb.Tid(binary.BigEndian.Uint64(tmpBuf[8+0:]))
	th.Lenm8 = uint64(binary.BigEndian.Uint64(tmpBuf[8+8:]))
	th.Status = zodb.TxnStatus(tmpBuf[8+16])

	if th.Lenm8 < txnHeaderFixSize {
		panic("too small txn record length")	// XXX
	}

        luser := binary.BigEndian.Uint16(tmpBuf[8+17:])
	ldesc := binary.BigEndian.Uint16(tmpBuf[8+19:])
	lext  := binary.BigEndian.Uint16(tmpBuf[8+21:])

	// XXX load strings only optionally
	lstr := int(luser) + int(ldesc) + int(lext)
	if lstr <= cap(th.stringsMem) {
		th.stringsMem = th.stringsMem[:lstr]
	} else {
		th.stringsMem = make([]byte, lstr)
	}

	nstr, err = r.ReadAt(th.stringsMem, pos + txnHeaderFixSize)
	// XXX EOF after txn header is not good
	if err != nil {
		return 0, &ErrTxnRecord{pos, "read", noEof(err)}
	}

	th.User = th.stringsMem[0:luser]

	th.Description = th.stringsMem[luser:luser+ldesc]
	th.Extension = th.stringsMem[luser+ldesc:luser+ldesc+lext]

	return txnHeaderFixSize + lstr, nil
}

// loadPrev reads and decodes previous transaction record header from a readerAt
// txnh should be already initialized by previous call to load()
func (txnh *TxnHeader) loadPrev(r io.ReaderAt, tmpBuf *[txnXHeaderFixSize]byte) error {
	if txnh.PrevLen == 0 {
		return io.EOF
	}

	return txnh.load(r, txnh.Pos - txnh.PrevLen, tmpBuf)
}

// loadNext reads and decodes next transaction record header from a readerAt
// txnh should be already initialized by previous call to load()
func (txnh *TxnHeader) loadNext(r io.ReaderAt, tmpBuf *[txnXHeaderFixSize]byte) error {
	return txnh.load(r, txnh.Pos + txnh.Len, tmpBuf)
}

// XXX do we need Decode when decode() is there?
func (th *TxnHeader) Decode(r io.ReaderAt, pos int64) (n int, err error) {
	var tmpBuf [txnXHeaderFixSize]byte
	return th.decode(r, pos, &tmpBuf)
}

func (th *TxnHeader) DecodePrev(r io.ReaderAt, pos int64) (posPrev int64, n int, err error) {
	var tmpBuf [txnXHeaderFixSize]byte
	return th.decodePrev(r, pos, &tmpBuf)
}


// decode reads and decodes data record header from a readerAt
// XXX io.ReaderAt -> *os.File  (if iface conv costly)
func (dh *DataHeader) decode(r io.ReaderAt, pos int64, tmpBuf *[dataHeaderSize]byte) error {
	n, err := r.ReadAt(tmpBuf[:], pos)
	// XXX vvv if EOF is after header - record is broken
	if n == dataHeaderSize {
		err = nil // we don't mind if it was EOF after full header read
	}

	if err != nil {
		return &ErrDataRecord{pos, "read", err}
	}

	dh.Oid = zodb.Oid(binary.BigEndian.Uint64(tmpBuf[0:]))	// XXX -> zodb.Oid.Decode() ?
	dh.Tid = zodb.Tid(binary.BigEndian.Uint64(tmpBuf[8:]))	// XXX -> zodb.Tid.Decode() ?
	dh.PrevDataRecPos = int64(binary.BigEndian.Uint64(tmpBuf[16:]))
	dh.TxnPos = int64(binary.BigEndian.Uint64(tmpBuf[24:]))
	verlen := binary.BigEndian.Uint16(tmpBuf[32:])
	dh.DataLen = binary.BigEndian.Uint64(tmpBuf[34:])

	if verlen != 0 {
		return &ErrDataRecord{pos, "invalid header", ErrVersionNonZero}
	}

	return nil
}

// XXX do we need Decode when decode() is there?
func (dh *DataHeader) Decode(r io.ReaderAt, pos int64) error {
	var tmpBuf [dataHeaderSize]byte
	return dh.decode(r, pos, &tmpBuf)
}



func OpenFileStorage(path string) (*FileStorage, error) {
	f, err := os.Open(path)	// XXX opens in O_RDONLY
	if err != nil {
		return nil, err	// XXX err more context ?
	}

	// check file magic
	var xxx [4]byte
	_, err = f.ReadAt(xxx[:], 0)
	if err != nil {
		return nil, err	// XXX err more context
	}
	if string(xxx[:]) != "FS21" {
		return nil, fmt.Errorf("%s: invalid magic %q", path, xxx)	// XXX err?
	}

	// TODO recreate index if missing / not sane
	// TODO verify index sane / topPos matches
	topPos, index, err := LoadIndexFile(path + ".index")
	if err != nil {
		panic(err)	// XXX err
	}

	// read tidMin/tidMax
	// FIXME support empty file case
	var txnhMin, txnhMax TxnHeader
	_, err = txnhMin.Decode(f, 4)
	if err != nil {
		return nil, err	// XXX +context
	}
	_, _, err = txnhMax.DecodePrev(f, topPos)
	if err != nil {
		return nil, err	// XXX +context
	}


	return &FileStorage{
			f: f,
			index: index,
			topPos: topPos,
			tidMin: txnhMin.Tid,
			tidMax: txnhMax.Tid,
		}, nil
}


func (fs *FileStorage) LastTid() zodb.Tid {
	panic("TODO")
}

// ErrXidLoad is returned when there is an error while loading xid
type ErrXidLoad struct {
	Xid	zodb.Xid
	Err	error
}

func (e *ErrXidLoad) Error() string {
	return fmt.Sprintf("loading %v: %v", e.Xid, e.Err)
}

func (fs *FileStorage) Load(xid zodb.Xid) (data []byte, tid zodb.Tid, err error) {
	// lookup in index position of oid data record within latest transaction who changed this oid
	dataPos, ok := fs.index.Get(xid.Oid)
	if !ok {
		// XXX drop oid from ErrOidMissing ?
		return nil, zodb.Tid(0), &ErrXidLoad{xid, zodb.ErrOidMissing{Oid: xid.Oid}}
	}

	dh := DataHeader{Tid: zodb.TidMax}
	tidBefore := xid.XTid.Tid
	if !xid.XTid.TidBefore {
		tidBefore++	// XXX recheck this is ok wrt overflow
	}

	// search backwards for when we first have data record with tid satisfying xid.XTid
	for {
		//prevTid := dh.Tid
		err = dh.Decode(fs.f, dataPos)
		if err != nil {
			return nil, zodb.Tid(0), &ErrXidLoad{xid, err}
		}

		// check data record consistency
		// TODO reenable
		// if dh.Oid != oid {
		// 	// ... header invalid:
		// 	return nil, zodb.Tid(0), &ErrXidLoad{xid, &ErrDataRecord{dataPos, "consistency check", "TODO unexpected oid")}
		// }

		// if dh.Tid >= prevTid { ... }
		// if dh.TxnPos >= dataPos - TxnHeaderSize { ... }
		// if dh.PrevDataRecPos >= dh.TxnPos - dataHeaderSize - 8 /* XXX */ { ... }

		if dh.Tid < tidBefore {
			break
		}

		// continue search
		dataPos = dh.PrevDataRecPos
		if dataPos == 0 {
			// no such oid revision
			return nil, zodb.Tid(0), &ErrXidLoad{xid, &zodb.ErrXidMissing{Xid: xid}}
		}
	}

	// found dh.Tid < tidBefore; check it really satisfies xid.XTid
	if !xid.XTid.TidBefore && dh.Tid != xid.XTid.Tid {
		// XXX unify with ^^^
		return nil, zodb.Tid(0), &ErrXidLoad{xid, &zodb.ErrXidMissing{Xid: xid}}
	}

	// even if we will scan back via backpointers, the tid returned should
	// be of first-found transaction
	tid = dh.Tid

	// scan via backpointers
	for dh.DataLen == 0 {
		var xxx [8]byte	// XXX escapes ?
		_, err = fs.f.ReadAt(xxx[:], dataPos + dataHeaderSize)
		if err != nil {
			panic(err)	// XXX
		}
		dataPos = int64(binary.BigEndian.Uint64(xxx[:]))
		err = dh.Decode(fs.f, dataPos)
		if err != nil {
			panic(err)	// XXX
		}
	}

	// now read actual data
	data = make([]byte, dh.DataLen)	// TODO -> slab ?
	n, err := fs.f.ReadAt(data, dataPos + dataHeaderSize)
	if n == len(data) {
		err = nil	// we don't mind to get EOF after full data read   XXX ok?
	}
	if err != nil {
		return nil, zodb.Tid(0), &ErrXidLoad{xid, err}
	}

	return data, tid, nil
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


type FileStorageIterator struct {
	txnPos int64	// current (?) transaction position

	tidMin, tidMax zodb.Tid	// iteration range: [tidMin, tidMax]
}

func (fsi *FileStorageIterator) NextTxn(txnInfo *zodb.TxnInfo) (dataIter zodb.IStorageRecordIterator, stop bool, err error) {
	// TODO
	return
}

func (fs *FileStorage) Iterate(tidMin, tidMax zodb.Tid) zodb.IStorageIterator {
	if tidMin < fs.tidMin {
		tidMin = fs.tidMin
	}
	if tidMin > fs.tidMax {
		// -> XXX empty
	}

/*
	(tidMin - fs.TidMin) vs (fs.TidMax - tidMin)

	// if forward
	iter := forwardIter{4, tidMin}
	for {
		iter.NextTxn(txnh, ...)
	}

	// txnh should have .Tid <= tidMin but next txn's .Tid is > tidMin
	posStart := iter.txnPos
	if t
*/

	return &FileStorageIterator{-1, tidMin, tidMax}	// XXX -1 ok ?
}
