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
// TODO link to format in zodb/py

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

// FileStorage is a ZODB storage which stores data in simple append-only file
// organized as transactional log.
type FileStorage struct {
	file	*os.File
	index	*fsIndex	// oid -> data record position in transaction which last changed oid
	topPos	int64		// position pointing just past last committed transaction
				// (= size(f) when no commit is in progress)

	// min/max tids committed
	tidMin, tidMax	zodb.Tid
}

// IStorage	XXX move ?
var _ zodb.IStorage = (*FileStorage)(nil)

// TxnHeader represents header of a transaction record
type TxnHeader struct {
	Pos	int64	// position of transaction start
	LenPrev	int64	// whole previous transaction record length
			// (0 if there is no previous txn record)
	Len	int64	// whole transaction record length

	// transaction metadata tself
	zodb.TxnInfo

	// underlying memory for header loading and for user/desc/extension
	workMem	[]byte
}

// DataHeader represents header of a data record
type DataHeader struct {
	Pos		int64	// position of data record
	Oid             zodb.Oid
	Tid             zodb.Tid
	PrevRevPos	int64	// position of this oid's previous-revision data record	XXX naming
	TxnPos          int64	// position of transaction record this data record belongs to
	//_		uint16	// 2-bytes with zero values. (Was version length.)
	DataLen		int64	// length of following data. if 0 -> following = 8 bytes backpointer
				// if backpointer == 0 -> oid deleted
	//Data            []byte
	//DataRecPos      uint64  // if Data == nil -> byte position of data record containing data

	// XXX include word0 ?
}

// on-disk sizes
const (
	Magic		= "FS21"

	TxnHeaderFixSize	= 8+8+1+2+2+2		// on-disk size without user/desc/ext strings
	txnXHeaderFixSize	= 8 + TxnHeaderFixSize	// with trail LenPrev from previous record

	DataHeaderSize		= 8+8+8+8+2+8

	// txn/data pos that are < vvv are for sure invalid
	txnValidFrom	= int64(len(Magic))
	dataValidFrom	= txnValidFrom + TxnHeaderFixSize
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

// ErrDataRecord is returned on data record read / decode errors
type ErrDataRecord struct {
	Pos	int64	// position of data record
	Subj	string	// about what .Err is
	Err	error	// actual error
}

func (e *ErrDataRecord) Error() string {
	return fmt.Sprintf("data record @%v: %v: %v", e.Pos, e.Subj, e.Err)
}

// // XXX -> zodb?
// var ErrVersionNonZero = errors.New("non-zero version")

var bugPosition = errors.New("software bug: invalid position")

// noEOF returns err, but changes io.EOF -> io.ErrUnexpectedEOF
func noEOF(err error) error {
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return err
}


// flags for TxnHeader.Load
type TxnLoadFlags int
const (
	LoadAll		TxnLoadFlags	= 0x00 // load whole transaction header
	LoadNoStrings			= 0x01 // do not load user/desc/ext strings
)

// Load reads and decodes transaction record header
// pos: points to transaction start
// no requirements are made to previous txnh state
// TODO describe what happens at EOF and when .LenPrev is still valid
func (txnh *TxnHeader) Load(r io.ReaderAt /* *os.File */, pos int64, flags TxnLoadFlags) error {
	if cap(txnh.workMem) < txnXHeaderFixSize {
		txnh.workMem = make([]byte, txnXHeaderFixSize)	// XXX or 0, ... ?
	}
	work := txnh.workMem[:txnXHeaderFixSize]

	txnh.Pos = pos
	txnh.Len = 0		// XXX recheck rules about error exit
	txnh.LenPrev = 0

	if pos < txnValidFrom {
		panic(&ErrTxnRecord{pos, "read", bugPosition})
	}

	decodeErr := func(format string, a ...interface{}) *ErrTxnRecord {
		return &ErrTxnRecord{pos, "decode", fmt.Errorf(format, a...)}
	}

	var n int
	var err error

	if pos - 8 >= txnValidFrom {
		// read together with previous's txn record redundand length
		n, err = r.ReadAt(work, pos - 8)
		n -= 8	// relative to pos
		if n >= 0 {
			lenPrev := 8 + int64(binary.BigEndian.Uint64(work[8-8:]))
			if lenPrev < TxnHeaderFixSize {
				return decodeErr("invalid txn prev record length: %v", lenPrev)
			}
			txnh.LenPrev = lenPrev
		}
	} else {
		// read only current txn without previous record length
		n, err = r.ReadAt(work[8:], pos)
	}


	if err != nil {
		if err == io.EOF && n == 0 {
			return err // end of stream
		}

		// EOF after txn header is not good - because at least
		// redundand lenght should be also there
		return &ErrTxnRecord{pos, "read", noEOF(err)}
	}

	txnh.Tid = zodb.Tid(binary.BigEndian.Uint64(work[8+0:]))
	if !txnh.Tid.Valid() {
		return decodeErr("invalid tid: %v", txnh.Tid)
	}

	tlen := 8 + int64(binary.BigEndian.Uint64(work[8+8:]))
	if tlen < TxnHeaderFixSize {
		return decodeErr("invalid txn record length: %v", tlen)
	}
	txnh.Len = tlen

	txnh.Status = zodb.TxnStatus(work[8+16])
	if !txnh.Status.Valid() {
		return decodeErr("invalid status: %v", txnh.Status)
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

	if flags & LoadNoStrings == 0 {
		err = txnh.loadStrings(r)
	}

	return err
}

// loadStrings makes sure strings that are part of transaction header are loaded
func (txnh *TxnHeader) loadStrings(r io.ReaderAt /* *os.File */) error {
	// XXX make it no-op if strings are already loaded?

	// we rely on Load leaving len(workMem) = sum of all strings length ...
	_, err := r.ReadAt(txnh.workMem, txnh.Pos + TxnHeaderFixSize)
	if err != nil {
		return &ErrTxnRecord{txnh.Pos, "read strings", noEOF(err)}
	}

	// ... and presetting x to point to appropriate places in .workMem .
	// so set len(x) = cap(x) to indicate strings are now loaded.
	txnh.User = txnh.User[:cap(txnh.User)]
	txnh.Description = txnh.Description[:cap(txnh.Description)]
	txnh.Extension = txnh.Extension[:cap(txnh.Extension)]

	return nil
}

// LoadPrev reads and decodes previous transaction record header
// txnh should be already initialized by previous call to load()
func (txnh *TxnHeader) LoadPrev(r io.ReaderAt, flags TxnLoadFlags) error {
	if txnh.LenPrev == 0 {
		return io.EOF
	}

	return txnh.Load(r, txnh.Pos - txnh.LenPrev, flags)
}

// LoadNext reads and decodes next transaction record header
// txnh should be already initialized by previous call to load()
func (txnh *TxnHeader) LoadNext(r io.ReaderAt, flags TxnLoadFlags) error {
	return txnh.Load(r, txnh.Pos + txnh.Len, flags)
}



// decode reads and decodes data record header
func (dh *DataHeader) load(r io.ReaderAt /* *os.File */, pos int64, tmpBuf *[DataHeaderSize]byte) error {
	if pos < dataValidFrom {
		panic(&ErrDataRecord{pos, "read", bugPosition})
	}

	dh.Pos = pos

	_, err := r.ReadAt(tmpBuf[:], pos)
	if err != nil {
		return &ErrDataRecord{pos, "read", noEOF(err)}
	}

	decodeErr := func(format string, a ...interface{}) *ErrDataRecord {
		return &ErrDataRecord{pos, "decode", fmt.Errorf(format, a...)}
	}

	// XXX also check oid.Valid() ?
	dh.Oid = zodb.Oid(binary.BigEndian.Uint64(tmpBuf[0:]))	// XXX -> zodb.Oid.Decode() ?
	dh.Tid = zodb.Tid(binary.BigEndian.Uint64(tmpBuf[8:]))	// XXX -> zodb.Tid.Decode() ?
	if !dh.Tid.Valid() {
		return decodeErr("invalid tid: %v", dh.Tid)
	}

	dh.PrevRevPos = int64(binary.BigEndian.Uint64(tmpBuf[16:]))
	dh.TxnPos = int64(binary.BigEndian.Uint64(tmpBuf[24:]))
	if dh.PrevRevPos < dataValidFrom {
		return decodeErr("invalid prev oid data position: %v", dh.PrevRevPos)
	}
	if dh.TxnPos < txnValidFrom {
		return decodeErr("invalid txn position: %v", dh.TxnPos)
	}

	if dh.TxnPos + TxnHeaderFixSize > pos {
		return decodeErr("txn position not decreasing: %v", dh.TxnPos)
	}
	if dh.PrevRevPos + DataHeaderSize > dh.TxnPos - 8 {
		return decodeErr("prev oid data position (%v) overlaps with txn (%v)", dh.PrevRevPos, dh.TxnPos)	// XXX wording
	}

	// XXX check PrevRevPos vs TxnPos overlap

	verlen := binary.BigEndian.Uint16(tmpBuf[32:])
	if verlen != 0 {
		return decodeErr("non-zero version: #%v", verlen)
	}

	dh.DataLen = int64(binary.BigEndian.Uint64(tmpBuf[34:]))
	if dh.DataLen < 0 {
		// XXX also check DataLen < max ?
		return decodeErr("invalid data len: %v", dh.DataLen)
	}


	return nil
}

// XXX do we need Load when load() is there?
func (dh *DataHeader) Load(r io.ReaderAt, pos int64) error {
	var tmpBuf [DataHeaderSize]byte
	return dh.load(r, pos, &tmpBuf)
}



func Open(path string) (*FileStorage, error) {
	f, err := os.Open(path)	// XXX opens in O_RDONLY
	if err != nil {
		return nil, err	// XXX err more context ?
	}

	// check file magic
	var xxx [len(Magic)]byte
	_, err = f.ReadAt(xxx[:], 0)
	if err != nil {
		return nil, err	// XXX err more context
	}
	if string(xxx[:]) != Magic {
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
	err = txnhMin.Load(f, txnValidFrom, LoadAll)	// XXX txnValidFrom here -> ?
	if err != nil {
		return nil, err	// XXX +context
	}
	err = txnhMax.Load(f, topPos, LoadAll)
	// XXX expect EOF but .PrevLen must be good
	if err != nil {
		return nil, err	// XXX +context
	}

	err = txnhMax.LoadPrev(f, LoadAll)
	if err != nil {
		// XXX
	}


	return &FileStorage{
			file: f,
			index: index,
			topPos: topPos,
			tidMin: txnhMin.Tid,
			tidMax: txnhMax.Tid,
		}, nil
}


func (fs *FileStorage) LastTid() zodb.Tid {
	// XXX check we have transactions at all
	// XXX what to return then?
	return fs.tidMax
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
		err = dh.Load(fs.file, dataPos)
		if err != nil {
			return nil, zodb.Tid(0), &ErrXidLoad{xid, err}
		}

		// TODO -> LoadPrev()
		// check data record consistency
		// TODO reenable
		// if dh.Oid != oid {
		// 	// ... header invalid:
		// 	return nil, zodb.Tid(0), &ErrXidLoad{xid, &ErrDataRecord{dataPos, "consistency check", "TODO unexpected oid")}
		// }

		// if dh.Tid >= prevTid { ... }
		// if dh.TxnPos >= dataPos - TxnHeaderSize { ... }
		// if dh.PrevDataRecPos >= dh.TxnPos - DataHeaderSize - 8 /* XXX */ { ... }

		if dh.Tid < tidBefore {
			break
		}

		// continue search
		dataPos = dh.PrevRevPos
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
		// XXX -> LoadBack() ?
		var xxx [8]byte	// XXX escapes ?
		_, err = fs.file.ReadAt(xxx[:], dataPos + DataHeaderSize)
		if err != nil {
			panic(err)	// XXX
		}
		dataPos = int64(binary.BigEndian.Uint64(xxx[:]))
		// XXX check dataPos < dh.Pos
		// XXX >= dataValidFrom
		err = dh.Load(fs.file, dataPos)
		if err != nil {
			panic(err)	// XXX
		}
	}

	// now read actual data
	data = make([]byte, dh.DataLen)	// TODO -> slab ?
	n, err := fs.file.ReadAt(data, dataPos + DataHeaderSize)
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
	err := fs.file.Close()
	if err != nil {
		return err
	}
	fs.file = nil
	return nil
}

func (fs *FileStorage) StorageName() string {
	return "FileStorage v1"
}


type forwardIter struct {
	fs *FileStorage

	Txnh	TxnHeader	// current transaction information
	TidMax	zodb.Tid	// iterate up to tid <= tidMax
}

func (fi *forwardIter) NextTxn(flags TxnLoadFlags) error {
	// XXX from what we start? how to yield 1st elem?
	err := fi.Txnh.LoadNext(fi.fs.file, flags)
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
	err = fsi.forwardIter.NextTxn(LoadAll)
	if err != nil {
		return nil, false, err	// XXX recheck
	}

	*txnInfo = fsi.forwardIter.Txnh.TxnInfo

	// TODO set dataIter

	return dataIter, false, nil
}

func (fs *FileStorage) Iterate(tidMin, tidMax zodb.Tid) zodb.IStorageIterator {
	/*
	if tidMin < fs.tidMin {
		tidMin = fs.tidMin
	}
	if tidMin > fs.tidMax {
		// -> XXX empty
	}

	(tidMin - fs.TidMin) vs (fs.TidMax - tidMin)

	if forward {
		iter = forwardIter{len(Magic), tidMin}
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
	*/
	return nil
}
