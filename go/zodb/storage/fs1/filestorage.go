// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

// Package fs1 provides so-called FileStorage version 1 ZODB storage.
//
// FileStorage is a single file organized as a simple append-only log of
// transactions with data changes. Every transaction record consists of:
//
// - transaction record header represented by TxnHeader,
// - several data records corresponding to modified objects,
// - redundant transaction length at the end of transaction record.
//
// Every data record consists of:
//
// - data record header represented by DataHeader,
// - actual data following the header.
//
// The "actual data" in addition to raw content, can be a back-pointer
// indicating that the actual content should be retrieved from a past revision.
//
// In addition to append-only transaction/data log, an index is automatically
// maintained mapping oid -> latest data record which modified this oid. The
// index is used to implement zodb.IStorage.Load for latest data without linear
// scan.
//
// The data format is bit-to-bit identical to FileStorage format implemented in ZODB/py.
// Please see the following links for original FileStorage format definition:
//
//	https://github.com/zopefoundation/ZODB/blob/a89485c1/src/ZODB/FileStorage/format.py
//	https://github.com/zopefoundation/ZODB/blob/a89485c1/src/ZODB/fstools.py
//
// The index format is interoperable with ZODB/py (index uses pickles which
// allow various valid encodings of a given object). Please see the following
// links for original FileStorage/py index definition:
//
//	https://github.com/zopefoundation/ZODB/blob/a89485c1/src/ZODB/fsIndex.py
//	https://github.com/zopefoundation/ZODB/commit/1bb14faf
//
// Unless one is doing something FileStorage-specific, it is adviced not to use
// fs1 package directly, and instead link-in lab.nexedi.com/kirr/neo/go/zodb/wks,
// open storage by zodb.OpenStorage and use it by way of zodb.IStorage interface.
//
// The fs1 package exposes all FileStorage data format details and most of
// internal workings so that it is possible to implement FileStorage-specific
// tools.
//
// See also package lab.nexedi.com/kirr/neo/go/zodb/storage/fs1/fs1tools and
// associated fs1 command for basic tools related to FileStorage maintenance.
package fs1

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/xcommon/xbufio"

	"lab.nexedi.com/kirr/go123/xerr"
)

// FileStorage is a ZODB storage which stores data in simple append-only file
// organized as transactional log.
//
// It is on-disk compatible with FileStorage from ZODB/py.
type FileStorage struct {
	file	*os.File
	index	*Index	// oid -> data record position in transaction which last changed oid

	// transaction headers for min/max transactions committed
	// (both with .Len=0 & .Tid=0 if database is empty)
	txnhMin	TxnHeader
	txnhMax TxnHeader
}

// IStorageDriver
var _ zodb.IStorageDriver = (*FileStorage)(nil)

func (fs *FileStorage) LastTid(_ context.Context) (zodb.Tid, error) {
	// XXX must be under lock
	return fs.txnhMax.Tid, nil // txnhMax.Tid = 0, if empty
}

func (fs *FileStorage) LastOid(_ context.Context) (zodb.Oid, error) {
	// XXX must be under lock
	lastOid, _ := fs.index.Last() // returns zero-value, if empty
	return lastOid, nil
}

// ErrXidLoad is returned when there is an error while loading xid
// XXX -> zodb (common bits)
type ErrXidLoad struct {
	Xid	zodb.Xid
	Err	error
}

func (e *ErrXidLoad) Error() string {
	return fmt.Sprintf("loading %v: %v", e.Xid, e.Err)
}

// XXX +Cause


// freelist(DataHeader)
var dhPool = sync.Pool{New: func() interface{} { return &DataHeader{} }}

// DataHeaderAlloc allocates DataHeader from freelist.
func DataHeaderAlloc() *DataHeader {
	return dhPool.Get().(*DataHeader)
}

// Free puts dh back into DataHeader freelist.
//
// Caller must not use dh after call to Free.
func (dh *DataHeader) Free() {
	dhPool.Put(dh)
}

func (fs *FileStorage) Load(_ context.Context, xid zodb.Xid) (buf *zodb.Buf, serial zodb.Tid, err error) {
	// lookup in index position of oid data record within latest transaction which changed this oid
	dataPos, ok := fs.index.Get(xid.Oid)
	if !ok {
		return nil, 0, &zodb.ErrOidMissing{Oid: xid.Oid}
	}

	// FIXME zodb.TidMax is only 7fff... tid from outside can be ffff...	-> TODO reject tid out of range

	// XXX go compiler cannot deduce dh should be on stack here
	//dh := DataHeader{Oid: xid.Oid, Tid: zodb.TidMax, PrevRevPos: dataPos}
	dh := DataHeaderAlloc()
	dh.Oid = xid.Oid
	dh.Tid = zodb.TidMax
	dh.PrevRevPos = dataPos
	//defer dh.Free()
	buf, serial, err = fs._Load(dh, xid)
	dh.Free()
	return buf, serial, err
}

func (fs *FileStorage) _Load(dh *DataHeader, xid zodb.Xid) (*zodb.Buf, zodb.Tid, error) {
	// search backwards for when we first have data record with tid satisfying xid.At
	for {
		err := dh.LoadPrevRev(fs.file)
		if err != nil {
			if err == io.EOF {
				// no such oid revision
				err = &zodb.ErrXidMissing{Xid: xid}
			} else {
				err = &ErrXidLoad{xid, err}
			}

			return nil, 0, err
		}

		if dh.Tid <= xid.At {
			break
		}
	}

	// even if we will scan back via backpointers, the serial returned should
	// be of first-found transaction
	serial := dh.Tid

	buf, err := dh.LoadData(fs.file)
	if err != nil {
		return nil, 0, &ErrXidLoad{xid, err}
	}
	if buf.Data == nil {
		// data was deleted
		return nil, 0, &zodb.ErrXidMissing{Xid: xid}
	}

	return buf, serial, nil
}

// --- ZODB-level iteration ---

// zIter is combined transaction/data-records iterator as specified by zodb.IStorage.Iterate
type zIter struct {
	iter Iter

	tidStop	zodb.Tid	// iterate up to tid <= tidStop | tid >= tidStop depending on iter.dir

	zFlags	zIterFlags

	// data header for data loading
	// ( NOTE: need to use separate dh because x.LoadData() changes x state
	//   while going through backpointers.
	//
	//   here to avoid allocations )
	dhLoading DataHeader

	datai	zodb.DataInfo // ptr to this will be returned by .NextData
	dataBuf	*zodb.Buf
}

type zIterFlags int
const (
	zIterEOF       zIterFlags = 1 << iota // EOF reached
	zIterPreloaded                        // data for this iteration was already preloaded
)

// NextTxn iterates to next/previous transaction record according to iteration direction
func (zi *zIter) NextTxn(_ context.Context) (*zodb.TxnInfo, zodb.IDataIterator, error) {
	switch {
	case zi.zFlags & zIterEOF != 0:
		return nil, nil, io.EOF

	case zi.zFlags & zIterPreloaded != 0:
		// first element is already there - preloaded by who initialized TxnIter
		zi.zFlags &= ^zIterPreloaded

	default:
		err := zi.iter.NextTxn(LoadAll)
		// XXX EOF ^^^ is not expected (range pre-cut to valid tids) ?
		if err != nil {
			return nil, nil, err
		}
	}

	if (zi.iter.Dir == IterForward && zi.iter.Txnh.Tid > zi.tidStop) ||
	   (zi.iter.Dir == IterBackward && zi.iter.Txnh.Tid < zi.tidStop) {
		zi.zFlags |= zIterEOF
		return nil, nil, io.EOF
	}

	return &zi.iter.Txnh.TxnInfo, zi, nil
}

// NextData iterates to next data record and loads data content
func (zi *zIter) NextData(_ context.Context) (*zodb.DataInfo, error) {
	err := zi.iter.NextData()
	if err != nil {
		return nil, err
	}

	zi.datai.Oid = zi.iter.Datah.Oid
	zi.datai.Tid = zi.iter.Datah.Tid

	// NOTE dh.LoadData() changes dh state while going through backpointers -
	// - need to use separate dh because of this.
	zi.dhLoading = zi.iter.Datah
	if zi.dataBuf != nil {
		zi.dataBuf.Release()
		zi.dataBuf = nil
	}
	zi.dataBuf, err = zi.dhLoading.LoadData(zi.iter.R)
	if err != nil {
		return nil, err
	}

	zi.datai.Data = zi.dataBuf.Data
	if zi.dhLoading.Tid != zi.datai.Tid {
		zi.datai.DataTidHint = zi.dhLoading.Tid
	} else {
		zi.datai.DataTidHint = 0
	}
	return &zi.datai, nil
}




// iterStartError is the iterator created when there are preparatory errors.
//
// this way we offload clients, besides handling NextTxn errors, from also
// handling error cases from Iterate.
//
// XXX bad idea? (e.g. it will prevent from devirtualizing what Iterate returns)
type iterStartError struct {
	err error
}

func (e *iterStartError) NextTxn(_ context.Context) (*zodb.TxnInfo, zodb.IDataIterator, error) {
	return nil, nil, e.err
}


// findTxnRecord finds transaction record with min(txn.tid): txn.tid >= tid
//
// if there is no such transaction returned error will be EOF.
func (fs *FileStorage) findTxnRecord(r io.ReaderAt, tid zodb.Tid) (TxnHeader, error) {
	// XXX read snapshot under lock

	// check for empty database
	if fs.txnhMin.Len == 0 {
		// empty database - no such record
		return TxnHeader{}, io.EOF
	}

	// now we know the database is not empty and thus txnh min & max are valid
	// clone them to unalias strings memory
	var tmin, tmax TxnHeader
	tmin.CloneFrom(&fs.txnhMin)
	tmax.CloneFrom(&fs.txnhMax)

	if tmax.Tid < tid {
		return TxnHeader{}, io.EOF // no such record
	}
	if tmin.Tid >= tid {
		return tmin, nil	// tmin satisfies
	}

	// now we know tid ∈ (tmin, tmax]
	// iterate and scan either from tmin or tmax, depending which way it is
	// likely closer, to searched tid.
	iter := &Iter{R: r}

	if (tid - tmin.Tid) < (tmax.Tid - tid) {
		//fmt.Printf("forward %.1f%%\n", 100 * float64(tid - tmin.Tid) / float64(tmax.Tid - tmin.Tid))
		iter.Dir = IterForward
		iter.Txnh = tmin // ok not to clone - memory is already ours
	} else {
		//fmt.Printf("backward %.1f%%\n", 100 * float64(tid - tmin.Tid) / float64(tmax.Tid - tmin.Tid))
		iter.Dir = IterBackward
		iter.Txnh = tmax // ok not to clone - ... ^^^
	}

	var txnhPrev TxnHeader

	for {
		txnhPrev = iter.Txnh // ok not to clone - we'll reload strings in the end

		err := iter.NextTxn(LoadNoStrings)
		if err != nil {
			return TxnHeader{}, noEOF(err)
		}

		if (iter.Dir == IterForward  && iter.Txnh.Tid >= tid) ||
		   (iter.Dir == IterBackward && iter.Txnh.Tid < tid) {
			break // found  (prev for backward)
		}
	}

	// found
	var txnhFound TxnHeader
	if iter.Dir == IterForward {
		txnhFound = iter.Txnh
	} else {
		txnhFound = txnhPrev
	}

	// load strings to make sure not to return txnh with strings data from
	// another transaction
	err := txnhFound.loadStrings(iter.R)
	if err != nil {
		return TxnHeader{}, noEOF(err)
	}

	return txnhFound, nil
}

// Iterate creates zodb-level iterator for tidMin..tidMax range
func (fs *FileStorage) Iterate(tidMin, tidMax zodb.Tid) zodb.ITxnIterator {
	// when iterating use IO optimized for sequential access
	fsSeq := xbufio.NewSeqReaderAt(fs.file)
	ziter := &zIter{iter: Iter{R: fsSeq}}
	iter := &ziter.iter

	// find first txn : txn.tid >= tidMin
	txnh, err := fs.findTxnRecord(fsSeq, tidMin)
	switch {
	case err == io.EOF:
		ziter.zFlags |= zIterEOF        // empty
		return ziter

	case err != nil:
		return &iterStartError{err}
	}

	// setup iter from what findTxnRecord found
	iter.Txnh = txnh
	iter.Datah.Pos = txnh.DataPos()      // XXX dup wrt Iter.NextTxn
	iter.Datah.DataLen = -DataHeaderSize // first iteration will go to first data record

	iter.Dir = IterForward	// TODO allow both ways iteration at ZODB level

	ziter.zFlags |= zIterPreloaded
	ziter.tidStop = tidMax

	return ziter
}

// --- open + rebuild index ---

// open opens FileStorage without loading index
func open(path string) (_ *FileStorage, err error) {
	fs := &FileStorage{}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fs.file = f
	defer func() {
		if err != nil {
			f.Close()	// XXX -> lclose
		}
	}()

	// check file magic
	fh := FileHeader{}
	err = fh.Load(f)
	if err != nil {
		return nil, err
	}

	// FIXME rework opening logic to support case when last txn was committed only partially

	// determine topPos from file size
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	topPos := fi.Size()

	// read tidMin/tidMax
	err = fs.txnhMin.Load(f, txnValidFrom, LoadAll)
	err = okEOF(err) // e.g. it is EOF when file is empty
	if err != nil {
		return nil, err
	}
	err = fs.txnhMax.Load(f, topPos, LoadAll)
	// expect EOF forward
	// FIXME ^^^ it will be no EOF if a txn was committed only partially
	if err != io.EOF {
		if err == nil {
			err = fmt.Errorf("%s: no EOF after topPos", f.Name())
		}
		return nil, fmt.Errorf("%s: %s", f.Name(), err)
	}

	// .LenPrev must be good or EOF backward
	switch fs.txnhMax.LenPrev {
	case -1:
		return nil, fmt.Errorf("%s: could not read LenPrev @%d (last transaction)", f.Name(), fs.txnhMax.Pos)
	case 0:
		// ok - EOF backward

	default:
		// .LenPrev is ok - read last previous record
		err = fs.txnhMax.LoadPrev(f, LoadAll)
		if err != nil {
			return nil, err
		}
	}

	return fs, nil
}

// Open opens FileStorage @path.
//
// TODO read-write support
func Open(ctx context.Context, path string) (_ *FileStorage, err error) {
	// open data file
	fs, err := open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			fs.file.Close()	// XXX lclose
		}
	}()

	// load-verify / rebuild index
	err = fs.loadIndex(ctx)
	if err != nil {
		log.Print(err)
		log.Printf("%s: index recompute...", path)
		fs.index, err = fs.computeIndex(ctx)
		if err != nil {
			return nil, err
		}

		// TODO if opened !ro -> .saveIndex()
	}

	return fs, nil
}

func (fs *FileStorage) Close() error {
	err := fs.file.Close()
	if err != nil {
		return err
	}
	fs.file = nil

	// TODO if opened !ro -> .saveIndex()

	return nil
}

func (fs *FileStorage) computeIndex(ctx context.Context) (index *Index, err error) {
	// XXX lock?
	fsSeq := xbufio.NewSeqReaderAt(fs.file)
	return BuildIndex(ctx, fsSeq, nil/*no progress; XXX somehow log it? */)
}

// loadIndex loads on-disk index to RAM and verifies it against data lightly
func (fs *FileStorage) loadIndex(ctx context.Context) (err error) {
	// XXX lock?
	defer xerr.Contextf(&err, "%s", fs.file.Name())

	index, err := LoadIndexFile(fs.file.Name() + ".index")
	if err != nil {
		return err
	}

	topPos := fs.txnhMax.Pos + fs.txnhMax.Len
	if index.TopPos != topPos {
		return fmt.Errorf("inconsistent index topPos: data=%d  index=%d", topPos, index.TopPos)
	}

	// quickly verify index sanity for last 100 transactions
	fsSeq := xbufio.NewSeqReaderAt(fs.file)
	_, err = index.Verify(ctx, fsSeq, 100, nil/*no progress*/)
	if err != nil {
		return err
	}

	fs.index = index
	return nil
}

// saveIndex flushes in-RAM index to disk
func (fs *FileStorage) saveIndex() (err error) {
	// XXX lock?
	defer xerr.Contextf(&err, "%s", fs.file.Name())

	err = fs.index.SaveFile(fs.file.Name() + ".index")
	if err != nil {
		return err
	}

	// XXX fsync here?
	return nil
}
