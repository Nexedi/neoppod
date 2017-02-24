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

	// transaction metadata tself
	zodb.TxnInfo

	// underlying memory for loading and for user/desc/extension
	workMem	[]byte
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


// Load reads and decodes transaction record header from a readerAt
// pos: points to transaction start
// no requirements are made to previous txnh state
// XXX io.ReaderAt -> *os.File  (if iface conv costly)
func (txnh *TxnHeader) Load(r io.ReaderAt, pos int64) error {
	if cap(txnh.workMem) < txnXHeaderFixSize {
		txnh.workMem = make([]byte, txnXHeaderFixSize)	// XXX or 0, ... ?
	}
	work := txnh.workMem[:txnXHeaderFixSize]	// XXX name

	txnh.Pos = pos

	if pos > 4 + 8 {	// XXX -> magic
		// read together with previous's txn record redundand length
		n, err = r.ReadAt(work, pos - 8)
		n -= 8	// relative to pos
		if n >= 0 {
			txnh.PrevLen = 8 + int64(binary.BigEndian.Uint64(work[8-8:]))
			if txnh.PrevLen < txnHeaderFixSize {
				panic("too small txn prev record length")	// XXX
			}
		}
	} else {
		n, err = r.ReadAt(work[8:], pos)
		txnh.PrevLen = 0
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

	txnh.Tid = zodb.Tid(binary.BigEndian.Uint64(work[8+0:]))
	txnh.Len = 8 + int64(binary.BigEndian.Uint64(work[8+8:]))
	txnh.Status = zodb.TxnStatus(work[8+16])

	if txnh.Len < txnHeaderFixSize {
		panic("too small txn record length")	// XXX
	}

        luser := binary.BigEndian.Uint16(work[8+17:])
	ldesc := binary.BigEndian.Uint16(work[8+19:])
	lext  := binary.BigEndian.Uint16(work[8+21:])

	// NOTE we encode whole strings length into len(.workMem)
	lstr := int(luser) + int(ldesc) + int(lext)
	if cap(txnh.workMem) < lstr {
		txnh.workMem = make([]byte, lstr)
	} else {
		txnh.workMem = txnh.workMem[:lstr]
	}

	work = txnh.workMem[:lstr]

	// NOTE we encode each x string length into cap(x)
	//      and set len(x) = 0 to indicate x is not loaded yet
	txnh.User	 = work[0:0:luser]
	txnh.Description = work[luser:luser:ldesc]
	txnh.Extension	 = work[luser+ldesc:luser+ldesc:lext]

	// XXX make strings loading optional
	txnh.loadString()

	return txnHeaderFixSize + lstr, nil
}

// loadStrings makes sure strings that are part of transaction header are loaded
func (txnh *TxnHeader) loadStrings(r io.ReaderAt) error {
	// XXX make it no-op if strings are already loaded?

	// we rely on Load leaving len(workMem) = sum of all strings length ...
	nstr, err = r.ReadAt(txnh.workMem, txnh.Pos + txnHeaderFixSize)
	if err != nil {
		return 0, &ErrTxnRecord{txnh.Pos, "read", noEof(err)}	// XXX -> "read strings" ?
	}

	// ... and presetting x to point to appropriate places in .workMem .
	// so set len(x) = cap(x) to indicate strings are now loaded.
	txnh.User = txnh.User[:cap(txnh.User)]
	txnh.Description = txnh.Description[:cap(txnh.Description)]
	txnh.Extension = txnh.Extension[:cap(txnh.Extension)]
}

// loadPrev reads and decodes previous transaction record header from a readerAt
// txnh should be already initialized by previous call to load()
func (txnh *TxnHeader) LoadPrev(r io.ReaderAt) error {
	if txnh.PrevLen == 0 {
		return io.EOF
	}

	return txnh.Load(r, txnh.Pos - txnh.PrevLen)
}

// loadNext reads and decodes next transaction record header from a readerAt
// txnh should be already initialized by previous call to load()
func (txnh *TxnHeader) LoadNext(r io.ReaderAt) error {
	return txnh.Load(r, txnh.Pos + txnh.Len)
}



// XXX -> Load ?
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
	err = txnhMin.Load(f, 4)
	if err != nil {
		return nil, err	// XXX +context
	}
	err = txnhMax.Load(f, topPos)
	// XXX expect EOF but .PrevLen must be good
	if err != nil {
		return nil, err	// XXX +context
	}

	err = txhhMax.LoadPrev(f)
	if err != nil {
		// XXX
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


type forwardIter struct {
	//Pos	int64		// current transaction position

	Txnh	TxnHeader	// current transaction information
	TidMax	zodb.Tid	// iterate up to tid <= tidMax
}

func (fi *forwardIter) NextTxn() error {
	// XXX from what we start? how to yield 1st elem?
	err := fi.Txnh.LoadNext()
	if err != nil {
		return err
	}

	// how to make sure last good txnh is preserved?
	if fi.Txnh.Tid > fi.TidMax {
		return io.EOF
	}

	return nil
}

// TODO backwardIter

type FileStorageIterator struct {
	forwardIter
	tidMin		zodb.Tid // iteration range: [tidMin, tidMax]
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

	(tidMin - fs.TidMin) vs (fs.TidMax - tidMin)

	if forward {
		iter = forwardIter{4, tidMin}
	} else {
		iter = backwardIter{fs.topPos, tidMin}
	}

	for {
		iter.NextTxn(txnh, ...)
	}

	// txnh should have .Tid <= tidMin but next txn's .Tid is > tidMin
	posStart := iter.txnPos
	if t

	return &FileStorageIterator{-1, tidMin, tidMax}	// XXX -1 ok ?
}
