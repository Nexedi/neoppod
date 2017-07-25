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

// Package fs1 provides so-called FileStorage v1 ZODB storage.
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
// index is used to implement zodb.IStorage.Load without linear scan.
//
// The data format is bit-to-bit identical to FileStorage format implemented in ZODB/py.
// Please see the following links for original FileStorage format definition:
//
//	https://github.com/zopefoundation/ZODB/blob/a89485c1/src/ZODB/FileStorage/format.py
//	https://github.com/zopefoundation/ZODB/blob/a89485c1/src/ZODB/fstools.py
//
// The index format is interoperable with ZODB/py (index uses pickles which
// allows various valid encodings of a given object). Please see the following
// links for original FileStorage index definition:
//
//	https://github.com/zopefoundation/ZODB/blob/a89485c1/src/ZODB/fsIndex.py
//	https://github.com/zopefoundation/ZODB/commit/1bb14faf
//
// Unless one is doing something FileStorage-specific, it is advices not to use
// fs1 package directly, and instead link-in lab.nexedi.com/kirr/neo/go/zodb/wks,
// open storage by zodb.OpenStorageURL and use it by way of zodb.IStorage interface.
//
// The fs1 package exposes all FileStorage data format details and most of
// internal workings so that it is possible to implement FileStorage-specific
// tools.
//
// Please see package lab.nexedi.com/kirr/neo/go/zodb/storage/fs1/fs1tools and
// associated fs1 command for basic tools related to FileStorage maintenance.
package fs1

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/xcommon/xbufio"

	"lab.nexedi.com/kirr/go123/xbytes"
)

// FileStorage is a ZODB storage which stores data in simple append-only file
// organized as transactional log.
type FileStorage struct {
	file	*os.File
	index	*Index	// oid -> data record position in transaction which last changed oid

	// transaction headers for min/max transactions committed
	// XXX keep loaded with LoadNoStrings ?
	txnhMin	TxnHeader
	txnhMax TxnHeader

	// XXX topPos = txnhMax.Pos + txnhMax.Len
	//topPos	int64	// position pointing just past last committed transaction
	//			// (= size(.file) when no commit is in progress)
}

// IStorage	XXX move ?
var _ zodb.IStorage = (*FileStorage)(nil)

// FileHeader represents FileStorage file header
type FileHeader struct {
	Magic [4]byte
}

// TxnHeader represents header of a transaction record
type TxnHeader struct {
	Pos	int64	// position of transaction start
	LenPrev	int64	// whole previous transaction record length
			// (-1 if there is no previous txn record) XXX see rules in Load
	Len	int64	// whole transaction record length	   XXX see rules in Load

	// transaction metadata itself
	zodb.TxnInfo

	// underlying memory for header loading and for user/desc/extension strings
	// invariant: after successful TxnHeader load len(.workMem) = lenUser + lenDesc + lenExt
	//            as specified by on-disk header
	workMem	[]byte
}

// DataHeader represents header of a data record
type DataHeader struct {
	Pos		int64	// position of data record start
	Oid             zodb.Oid
	Tid             zodb.Tid
	// XXX -> .PosPrevRev  .PosTxn  .LenData
	PrevRevPos	int64	// position of this oid's previous-revision data record	XXX naming
	TxnPos          int64	// position of transaction record this data record belongs to
	//_		uint16	// 2-bytes with zero values. (Was version length.)
	DataLen		int64	// length of following data. if 0 -> following = 8 bytes backpointer
				// if backpointer == 0 -> oid deleted
	//Data            []byte
	//DataRecPos      uint64  // if Data == nil -> byte position of data record containing data

	// XXX include word0 ?

	// underlying memory for header loading (to avoid allocations)
	workMem [DataHeaderSize]byte
}

const (
	Magic = "FS21"	// every FileStorage file starts with this

	// on-disk sizes
	TxnHeaderFixSize	= 8+8+1+2+2+2		// without user/desc/ext strings
	txnXHeaderFixSize	= 8 + TxnHeaderFixSize	// ^^^ with trail LenPrev from previous record
	DataHeaderSize		= 8+8+8+8+2+8

	// txn/data pos that are < vvv are for sure invalid
	txnValidFrom	= int64(len(Magic))
	dataValidFrom	= txnValidFrom + TxnHeaderFixSize
)

// ErrTxnRecord is returned on transaction record read / decode errors
type ErrTxnRecord struct {
	Pos	int64	// position of transaction record
	Subj	string	// about what .Err is
	Err	error	// actual error
}

func (e *ErrTxnRecord) Error() string {
	return fmt.Sprintf("transaction record @%v: %v: %v", e.Pos, e.Subj, e.Err)
}

// err creates ErrTxnRecord for transaction located at txnh.Pos
func (txnh *TxnHeader) err(subj string, err error) error {
	return &ErrTxnRecord{txnh.Pos, subj, err}
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

// err creates ErrDataRecord for data record located at dh.Pos
// XXX add link to containing txn? (check whether we can do it on data access) ?
func (dh *DataHeader) err(subj string, err error) error {
	return &ErrDataRecord{dh.Pos, subj, err}
}


// xerr is an interface for something which can create errors
// it is used by TxnHeader and DataHeader to create appropriate errors with their context
type xerr interface {
	err(subj string, err error) error
}

// errf is syntactic shortcut for err and fmt.Errorf
func errf(e xerr, subj, format string, a ...interface{}) error {
	return e.err(subj, fmt.Errorf(format, a...))
}

// decodeErr is syntactic shortcut for errf("decode", ...)
// TODO in many places "decode" -> "selfcheck"
func decodeErr(e xerr, format string, a ...interface{}) error {
	return errf(e, "decode", format, a...)
}

// bug panics with errf("bug", ...)
func bug(e xerr, format string, a ...interface{}) {
	panic(errf(e, "bug", format, a...))
}


// XXX -> xio ?
// noEOF returns err, but changes io.EOF -> io.ErrUnexpectedEOF
func noEOF(err error) error {
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return err
}

// okEOF returns err, but changes io.EOF -> nil
func okEOF(err error) error {
	if err == io.EOF {
		err = nil
	}
	return err
}

// --- File header ---

// Load reads and decodes file header.
func (fh *FileHeader) Load(r io.ReaderAt) error {
	_, err := r.ReadAt(fh.Magic[:], 0)
	err = okEOF(err)
	if err != nil {
		return fh.err("read", err)
		//return  err	// XXX err more context
	}
	if string(fh.Magic[:]) != Magic {
		//return fmt.Errorf("%s: invalid magic %q", path, fh.Magic)	// XXX -> decode err
		return decodeErr(fh, "invalid magic %q", fh.Magic)
	}

	return nil
}

// --- Transaction record ---

// HeaderLen returns whole transaction header length including variable part.
// NOTE: data records start right after transaction header.
func (txnh *TxnHeader) HeaderLen() int64 {
	return TxnHeaderFixSize + int64(len(txnh.workMem))
}

// DataPos returns start position of data inside transaction record
func (txnh *TxnHeader) DataPos() int64 {
	return txnh.Pos + txnh.HeaderLen()
}

// DataLen returns length of all data inside transaction record container
func (txnh *TxnHeader) DataLen() int64 {
	return txnh.Len - txnh.HeaderLen() - 8 /* trailer redundant length */
}

// CloneFrom copies txnh2 to txnh making sure underlying slices (.workMem .User
// .Desc ...) are not shared.
func (txnh *TxnHeader) CloneFrom(txnh2 *TxnHeader) {
	workMem := txnh.workMem
	lwork2 := len(txnh2.workMem)
	workMem = xbytes.Realloc(workMem, lwork2)
	*txnh = *txnh2
	// now unshare slices
	txnh.workMem = workMem
	copy(workMem, txnh2.workMem)

	// FIXME handle case when strings were already loaded -> set len properly
	luser := cap(txnh2.User)
	xdesc := luser + cap(txnh2.Description)
	xext  := xdesc + cap(txnh2.Extension)
	txnh.User	 = workMem[0:0:luser]
	txnh.Description = workMem[luser:luser:xdesc]
	txnh.Extension   = workMem[xdesc:xdesc:xext]
}

// flags for TxnHeader.Load
type TxnLoadFlags int
const (
	LoadAll		TxnLoadFlags	= 0x00 // load whole transaction header
	LoadNoStrings			= 0x01 // do not load user/desc/ext strings
)

// Load reads and decodes transaction record header.
//
// pos: points to transaction start
// no prerequisite requirements are made to previous txnh state
// TODO describe what happens at EOF and when .LenPrev is still valid
//
// rules for Len/LenPrev returns:
// Len ==  0			transaction header could not be read
// Len == -1			EOF forward
// Len >= TxnHeaderFixSize	transaction was read normally
//
// LenPrev == 0			prev record length could not be read
// LenPrev == -1		EOF backward
// LenPrev >= TxnHeaderFixSize	LenPrev was read/checked normally
func (txnh *TxnHeader) Load(r io.ReaderAt, pos int64, flags TxnLoadFlags) error {
	if cap(txnh.workMem) < txnXHeaderFixSize {
		txnh.workMem = make([]byte, txnXHeaderFixSize, 256 /* to later avoid allocation for typical strings */)
	}
	work := txnh.workMem[:txnXHeaderFixSize]

	// XXX recheck rules about error exit
	txnh.Pos = pos
	txnh.Len = 0		// read error
	txnh.LenPrev = 0	// read error

	if pos < txnValidFrom {
		bug(txnh, "Load() on invalid position")
	}

	var n int
	var err error

	if pos - 8 >= txnValidFrom {
		// read together with previous's txn record redundant length
		n, err = r.ReadAt(work, pos - 8)
		n -= 8	// relative to pos
		if n >= 0 {
			lenPrev := 8 + int64(binary.BigEndian.Uint64(work[8-8:]))
			if lenPrev < TxnHeaderFixSize {
				return decodeErr(txnh, "invalid prev record length: %v", lenPrev)
			}
			posPrev := txnh.Pos - lenPrev
			if posPrev < txnValidFrom {
				return decodeErr(txnh, "prev record length goes beyond valid area: %v", lenPrev)
			}
			if posPrev < txnValidFrom + TxnHeaderFixSize && posPrev != txnValidFrom {
				return decodeErr(txnh, "prev record does not land exactly at valid area start: %v", posPrev)
			}
			txnh.LenPrev = lenPrev
		}
	} else {
		// read only current txn without previous record length
		n, err = r.ReadAt(work[8:], pos)
		txnh.LenPrev = -1	// EOF backward
	}


	if err != nil {
		if err == io.EOF && n == 0 {
			txnh.Len = -1	// EOF forward
			return err	// end of stream
		}

		// EOF after txn header is not good - because at least
		// redundant length should be also there
		return txnh.err("read", noEOF(err))
	}

	txnh.Tid = zodb.Tid(binary.BigEndian.Uint64(work[8+0:]))
	if !txnh.Tid.Valid() {
		return decodeErr(txnh, "invalid tid: %v", txnh.Tid)
	}

	tlen := 8 + int64(binary.BigEndian.Uint64(work[8+8:]))
	if tlen < TxnHeaderFixSize {
		return decodeErr(txnh, "invalid txn record length: %v", tlen)
	}
	// XXX also check tlen to not go beyond file size ?
	txnh.Len = tlen

	txnh.Status = zodb.TxnStatus(work[8+16])
	if !txnh.Status.Valid() {
		return decodeErr(txnh, "invalid status: %v", txnh.Status)
	}


        luser := binary.BigEndian.Uint16(work[8+17:])
	ldesc := binary.BigEndian.Uint16(work[8+19:])
	lext  := binary.BigEndian.Uint16(work[8+21:])

	lstr := int(luser) + int(ldesc) + int(lext)
	if TxnHeaderFixSize + int64(lstr) + 8 > txnh.Len {
		return decodeErr(txnh, "strings overlap with txn boundary: %v / %v", lstr, txnh.Len)
	}

	// NOTE we encode whole strings length into len(.workMem)
	txnh.workMem = xbytes.Realloc(txnh.workMem, lstr)

	// NOTE we encode each x string length into cap(x)
	//      and set len(x) = 0 to indicate x is not loaded yet
	//println("workmem len:", len(txnh.workMem), "cap:", cap(txnh.workMem))
	//println("luser:", luser)
	//println("ldesc:", ldesc)
	//println("lext: ", lext)
	xdesc := luser + ldesc
	xext  := xdesc + lext
	txnh.User	 = txnh.workMem[0:0:luser]
	txnh.Description = txnh.workMem[luser:luser:xdesc]
	txnh.Extension	 = txnh.workMem[xdesc:xdesc:xext]

	if flags & LoadNoStrings == 0 {
		err = txnh.loadStrings(r)
	}

	return err
}

// loadStrings makes sure strings that are part of transaction header are loaded
func (txnh *TxnHeader) loadStrings(r io.ReaderAt) error {
	// XXX make it no-op if strings are already loaded?

	// we rely on Load leaving len(workMem) = sum of all strings length ...
	_, err := r.ReadAt(txnh.workMem, txnh.Pos + TxnHeaderFixSize)
	if err != nil {
		return txnh.err("read strings", noEOF(err))
	}

	// ... and presetting x to point to appropriate places in .workMem .
	// so set len(x) = cap(x) to indicate strings are now loaded.
	txnh.User = txnh.User[:cap(txnh.User)]
	txnh.Description = txnh.Description[:cap(txnh.Description)]
	txnh.Extension = txnh.Extension[:cap(txnh.Extension)]

	return nil
}

// LoadPrev reads and decodes previous transaction record header.
//
// prerequisite: txnh .Pos, .LenPrev and .Len are initialized:	XXX (.Len for .Tid)
//   - by successful call to Load() initially			XXX but EOF also works
//   - by subsequent successful calls to LoadPrev / LoadNext	XXX recheck
func (txnh *TxnHeader) LoadPrev(r io.ReaderAt, flags TxnLoadFlags) error {
	lenPrev := txnh.LenPrev
	switch lenPrev {	// XXX recheck states for: 1) LenPrev load error  2) EOF
	case 0:
		bug(txnh, "LoadPrev() when .LenPrev == error")
	case -1:
		return io.EOF
	}

	lenCur := txnh.Len
	tidCur := txnh.Tid

	// here we know: Load already checked txnh.Pos - lenPrev to be valid position
	err := txnh.Load(r, txnh.Pos - lenPrev, flags)
	if err != nil {
		// EOF forward is unexpected here
		return noEOF(err)
	}

	if txnh.Len != lenPrev {
		return decodeErr(txnh, "head/tail lengths mismatch: %v, %v", txnh.Len, lenPrev)
	}

	// check tid↓ if we had txnh for "cur" loaded
	if lenCur > 0 && txnh.Tid >= tidCur {
		return decodeErr(txnh, "tid monitonity broken: %v  ; next: %v", txnh.Tid, tidCur)
	}

	return nil
}

// LoadNext reads and decodes next transaction record header.
// prerequisite: txnh .Pos and .Len should be already initialized by:	XXX also .Tid
//   - previous successful call to Load() initially		XXX ^^^
//   - TODO
func (txnh *TxnHeader) LoadNext(r io.ReaderAt, flags TxnLoadFlags) error {
	lenCur := txnh.Len
	posCur := txnh.Pos
	switch lenCur {
	case 0:
		bug(txnh, "LoadNext() when .Len == error")
	case -1:
		return io.EOF
	}

	// valid .Len means txnh was read ok
	tidCur := txnh.Tid

	err := txnh.Load(r, txnh.Pos + lenCur, flags)

	// before checking loading error for next txn, let's first check redundant length
	// NOTE also: err could be EOF
	if txnh.LenPrev != 0 && txnh.LenPrev != lenCur {
		t := &TxnHeader{Pos: posCur} // txn for which we discovered problem
		return decodeErr(t, "head/tail lengths mismatch: %v, %v", lenCur, txnh.LenPrev)
	}

	if err != nil {
		return err
	}

	// check tid↑
	if txnh.Tid <= tidCur {
		return decodeErr(txnh, "tid monotonity broken: %v  ; prev: %v", txnh.Tid, tidCur)
	}

	return nil
}

// --- Data record ---

// Len returns whole data record length.
func (dh *DataHeader) Len() int64 {
	dataLen := dh.DataLen
	if dataLen == 0 {
		// XXX -> .DataLen() ?
		dataLen = 8 // back-pointer | oid removal
	}

	return DataHeaderSize + dataLen
}


// Load reads and decodes data record header.
// pos: points to data header start
// no prerequisite requirements are made to previous dh state
func (dh *DataHeader) Load(r io.ReaderAt, pos int64) error {
	dh.Pos = pos
	// XXX .Len = 0		= read error ?

	if pos < dataValidFrom {
		bug(dh, "Load() on invalid position")
	}

	_, err := r.ReadAt(dh.workMem[:], pos)
	if err != nil {
		return dh.err("read", noEOF(err))
	}

	// XXX also check oid.Valid() ?
	dh.Oid = zodb.Oid(binary.BigEndian.Uint64(dh.workMem[0:]))	// XXX -> zodb.Oid.Decode() ?
	dh.Tid = zodb.Tid(binary.BigEndian.Uint64(dh.workMem[8:]))	// XXX -> zodb.Tid.Decode() ?
	if !dh.Tid.Valid() {
		return decodeErr(dh, "invalid tid: %v", dh.Tid)
	}

	dh.PrevRevPos = int64(binary.BigEndian.Uint64(dh.workMem[16:]))
	dh.TxnPos = int64(binary.BigEndian.Uint64(dh.workMem[24:]))
	if dh.TxnPos < txnValidFrom {
		return decodeErr(dh, "invalid txn position: %v", dh.TxnPos)
	}

	if dh.TxnPos + TxnHeaderFixSize > pos {
		return decodeErr(dh, "txn position not decreasing: %v", dh.TxnPos)
	}
	if dh.PrevRevPos != 0 {	// zero means there is no previous revision
		if dh.PrevRevPos < dataValidFrom {
			return decodeErr(dh, "invalid prev revision position: %v", dh.PrevRevPos)
		}
		if dh.PrevRevPos + DataHeaderSize > dh.TxnPos - 8 {
			return decodeErr(dh, "prev revision position (%v) overlaps with txn (%v)", dh.PrevRevPos, dh.TxnPos)
		}
	}

	verlen := binary.BigEndian.Uint16(dh.workMem[32:])
	if verlen != 0 {
		return decodeErr(dh, "non-zero version: #%v", verlen)
	}

	dh.DataLen = int64(binary.BigEndian.Uint64(dh.workMem[34:]))
	if dh.DataLen < 0 {
		// XXX also check DataLen < max ?
		return decodeErr(dh, "invalid data len: %v", dh.DataLen)
	}

	return nil
}

// LoadPrevRev reads and decodes previous revision data record header.
// prerequisite: dh .Oid .Tid .PrevRevPos are initialized:
//   - TODO describe how
// when there is no previous revision: io.EOF is returned
func (dh *DataHeader) LoadPrevRev(r io.ReaderAt) error {
	if dh.PrevRevPos == 0 {
		return io.EOF	// no more previous revisions
	}

	posCur := dh.Pos

	err := dh.loadPrevRev(r)
	if err != nil {
		// data record @...: loading prev rev: data record @...: ...
		err = &ErrDataRecord{posCur, "loading prev rev", err}
	}
	return err
}

func (dh *DataHeader) loadPrevRev(r io.ReaderAt) error {
	oid := dh.Oid
	tid := dh.Tid

	err := dh.Load(r, dh.PrevRevPos)
	if err != nil {
		return err
	}

	if dh.Oid != oid {
		// XXX vvv valid only if ErrDataRecord prints oid
		return decodeErr(dh, "oid mismatch")
	}

	if dh.Tid >= tid {
		// XXX vvv valid only if ErrDataRecord prints tid
		return decodeErr(dh, "tid mismatch")
	}

	return nil
}

// LoadBack reads and decodes data header for revision linked via back-pointer.
// prerequisite: dh XXX     .DataLen == 0
// if link is to zero (means deleted record) io.EOF is returned
func (dh *DataHeader) LoadBack(r io.ReaderAt) error {
	if dh.DataLen != 0 {
		bug(dh, "LoadBack() on non-backpointer data header")
	}

	_, err := r.ReadAt(dh.workMem[:8], dh.Pos + DataHeaderSize)
	if err != nil {
		return dh.err("read data", noEOF(err))
	}

	backPos := int64(binary.BigEndian.Uint64(dh.workMem[0:]))
	if backPos == 0 {
		return io.EOF	// oid was deleted
	}
	if backPos < dataValidFrom {
		return decodeErr(dh, "invalid backpointer: %v", backPos)
	}
	if backPos + DataHeaderSize > dh.TxnPos - 8 {
		return decodeErr(dh, "backpointer (%v) overlaps with txn (%v)", backPos, dh.TxnPos)
	}

	posCur := dh.Pos
	tid := dh.Tid

	// TODO compare this with loadPrevRev() way
	err = func() error {
		err := dh.Load(r, backPos)
		if err != nil {
			return err
		}

		// XXX also dh.Oid == oid ?
		//     but in general back pointer might point to record with different oid
		if dh.Tid >= tid {
			return decodeErr(dh, "tid not decreasing")
		}

		return err
	}()

	if err != nil {
		err = &ErrDataRecord{posCur, "loading back rev", err}
	}

	return err
}

// LoadNext reads and decodes data header for next data record in the same transaction.
// prerequisite: dh .Pos .DataLen are initialized
// when there is no more data records: io.EOF is returned
func (dh *DataHeader) LoadNext(r io.ReaderAt, txnh *TxnHeader) error {
	err := dh.loadNext(r, txnh)
	if err != nil && err != io.EOF {
		err = txnh.err("iterating", err)
	}
	return err
}

func (dh *DataHeader) loadNext(r io.ReaderAt, txnh *TxnHeader) error {
	// position of txn tail - right after last data record byte
	txnTailPos := txnh.Pos + txnh.Len - 8

	// NOTE we know nextPos does not overlap txnTailPos - it was checked by
	// previous LoadNext()
	nextPos := dh.Pos + dh.Len()
	if nextPos == txnTailPos {
		return io.EOF
	}

	if nextPos + DataHeaderSize > txnTailPos {
		return &ErrDataRecord{nextPos, "decode", fmt.Errorf("data record header overlaps txn boundary")}	// XXX
	}

	err := dh.Load(r, nextPos)
	if err != nil {
		return err
	}

	if dh.Tid != txnh.Tid {
		return decodeErr(dh, "data.tid != txn.Tid")	// XXX
	}
	if dh.TxnPos != txnh.Pos {
		return decodeErr(dh, "data.txnPos != txn.Pos")	// XXX
	}
	if dh.Pos + dh.Len() > txnTailPos {
		return decodeErr(dh, "data record overlaps txn boundary")	// XXX
	}

	return nil
}

// LoadData loads data for the data record taking backpointers into account.
// Data is loaded into *buf, which, if needed, is reallocated to hold whole loading data size.
// NOTE on success dh state is changed to data header of original data transaction
// NOTE "deleted" records are indicated via returning *buf=nil
// TODO buf -> slab
func (dh *DataHeader) LoadData(r io.ReaderAt, buf *[]byte)  error {
	// scan via backpointers
	for dh.DataLen == 0 {
		err := dh.LoadBack(r)
		if err != nil {
			if err == io.EOF {
				*buf = nil // deleted
				return nil
			}
			return err	// XXX recheck
		}
	}

	// now read actual data
	*buf = xbytes.Realloc64(*buf, dh.DataLen)
	_, err := r.ReadAt(*buf, dh.Pos + DataHeaderSize)
	if err != nil {
		return dh.err("read data", noEOF(err))	// XXX recheck
	}

	return nil
}

// --- FileStorage ---

func (fs *FileStorage) StorageName() string {
	return "FileStorage v1"
}

// open opens FileStorage without loading index
func open(path string) (*FileStorage, error) {
	fs := &FileStorage{}

	f, err := os.Open(path)	// XXX opens in O_RDONLY
	if err != nil {
		return nil, err	// XXX err more context ?
	}
	fs.file = f

	// check file magic
	var xxx [len(Magic)]byte
	_, err = f.ReadAt(xxx[:], 0)
	if err != nil {
		return nil, err	// XXX err more context
	}
	if string(xxx[:]) != Magic {
		return nil, fmt.Errorf("%s: invalid magic %q", path, xxx)	// XXX err?
	}

/*
	// TODO recreate index if missing / not sane (cancel this job on ctx.Done)
	// TODO verify index sane / topPos matches
	topPos, index, err := LoadIndexFile(path + ".index")
	if err != nil {
		panic(err)	// XXX err
	}
	fs.index = index
*/

	// determine topPos from file size
	// if it is invalid (e.g. a transaction committed only half-way) we'll catch it
	// while loading/recreating index	XXX recheck this logic
	fi, err := f.Stat()
	if err != nil {
		return nil, err	// XXX err ctx
	}
	topPos := fi.Size()

	// read tidMin/tidMax
	// FIXME support empty file case
	err = fs.txnhMin.Load(f, txnValidFrom, LoadAll)	// XXX txnValidFrom here -> ?
	if err != nil {
		return nil, err	// XXX +context
	}
	err = fs.txnhMax.Load(f, topPos, LoadAll)
	// expect EOF but .LenPrev must be good
	// FIXME ^^^ it will be no EOF if a txn was committed only partially
	if err != io.EOF {
		if err == nil {
			err = fmt.Errorf("no EOF after topPos")	// XXX err context
		}
		return nil, err	// XXX +context
	}
	if fs.txnhMax.LenPrev <= 0 {
		panic("could not read LenPrev @topPos")	// XXX err
	}

	err = fs.txnhMax.LoadPrev(f, LoadAll)
	if err != nil {
		panic(err)	// XXX
	}


	return fs, nil
}

// Open opens FileStorage XXX text
func Open(ctx context.Context, path string) (*FileStorage, error) {
	fs, err := open(path)
	if err != nil {
		return nil, err
	}

	// TODO recreate index if missing / not sane (cancel this job on ctx.Done)
	index, err := LoadIndexFile(path + ".index")
	if err != nil {
		panic(err)	// XXX err
	}

	// TODO verify index sane / topPos matches
	if index.TopPos != fs.txnhMax.Pos + fs.txnhMax.Len {
		panic("inconsistent index topPos")	// XXX
	}

	fs.index = index

	return fs, nil
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


func (fs *FileStorage) LastTid() (zodb.Tid, error) {
	// XXX check we have transactions at all
	// XXX what to return if not?
	// XXX must be under lock
	return fs.txnhMax.Tid, nil	// XXX error always nil ?
}

func (fs *FileStorage) LastOid() (zodb.Oid, error) {
	// XXX check we have objects at all?
	// XXX what to return if not?
	// XXX must be under lock
	// XXX what if an oid was deleted?
	lastOid, _ := fs.index.Last() // returns zero-value, if empty
	return lastOid, nil	// XXX error always nil?
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
		return nil, 0, &zodb.ErrOidMissing{Oid: xid.Oid}
	}

	// FIXME zodb.TidMax is only 7fff... tid from outside can be ffff...
	dh := DataHeader{Oid: xid.Oid, Tid: zodb.TidMax, PrevRevPos: dataPos}
	tidBefore := xid.XTid.Tid
	if !xid.XTid.TidBefore {
		tidBefore++	// XXX recheck this is ok wrt overflow
	}

	// search backwards for when we first have data record with tid satisfying xid.XTid
	for dh.Tid >= tidBefore {
		err = dh.LoadPrevRev(fs.file)
		if err != nil {
			if err == io.EOF {
				// no such oid revision
				err = &zodb.ErrXidMissing{Xid: xid}
			} else {
				err = &ErrXidLoad{xid, err}
			}

			return nil, 0, err
		}
	}

	// found dh.Tid < tidBefore; check it really satisfies xid.XTid
	if !xid.XTid.TidBefore && dh.Tid != xid.XTid.Tid {
		return nil, 0, &zodb.ErrXidMissing{Xid: xid}
	}

	// even if we will scan back via backpointers, the tid returned should
	// be of first-found transaction
	tid = dh.Tid

	// TODO data -> slab
	err = dh.LoadData(fs.file, &data)
	if err != nil {
		return nil, 0, &ErrXidLoad{xid, err}
	}
	if data == nil {
		// data was deleted
		// XXX or allow this and return via data=nil ?
		return nil, 0, &zodb.ErrXidMissing{Xid: xid}
	}

	return data, tid, nil
}

// --- raw iteration ---

// Iter is combined 2-level iterator over transaction and data records
type Iter struct {
	fsSeq *xbufio.SeqReaderAt
	Flags	iterFlags	// XXX iterate forward (> 0) / backward (< 0) / EOF reached (== 0)

	Txnh	TxnHeader	// current transaction record information
	Datah	DataHeader	// current data record information
}


// NextTxn iterates to next/previous transaction record according to iteration direction.
// The data header is reset to iterate inside transaction record that became current.
func (it *Iter) NextTxn(flags TxnLoadFlags) error {
	var err error
	if it.Flags & iterDir != 0 {
		err = it.Txnh.LoadNext(it.fsSeq, flags)
	} else {
		err = it.Txnh.LoadPrev(it.fsSeq, flags)
	}

	//fmt.Println("loaded:", it.Txnh.Tid)
	if err != nil {
		// reset .Datah to be invalid (just in case)
		it.Datah.Pos = 0
		it.Datah.DataLen = 0
	} else {
		// set .Datah to iterate over .Txnh
		it.Datah.Pos = it.Txnh.DataPos()
		it.Datah.DataLen = -DataHeaderSize // first iteration will go to first data record
	}

	return err
}

// NextData iterates to next data record header inside current transaction
func (it *Iter) NextData() error {
	return it.Datah.LoadNext(it.fsSeq, &it.Txnh)
}


// IterateRaw ... XXX
func (fs *FileStorage) IterateRaw(dir/*XXX fwd/back*/) *Iter {
	// when iterating use IO optimized for sequential access
	fsSeq := xbufio.NewSeqReaderAt(fs.file)
	// XXX setup .TxnIter.dir and start
	it := &Iter{fsSeq: fsSeq}	// XXX ...
	//it.DataIter.Txnh = &iter.txnIter.Txnh
	return it
}


// --- zodb.IStorage iteration ---

type iterFlags int
const (
	iterDir       iterFlags = 1 << iota // iterate forward (1) or backward (0)
	iterEOF                             // EOF reached
	iterPreloaded                       // data for this iteration was already preloaded
)

// zIter is transaction/data-records iterator as specified by zodb.IStorage
type zIter struct {
	iter Iter

	TidStop	zodb.Tid	// iterate up to tid <= tidStop | tid >= tidStop depending on .dir

	Flags	iterFlags

	// data header for data loading
	// ( NOTE: need to use separate dh because x.LoadData() changes x state
	//   while going through backpointers.
	//
	//   here to avoid allocations )
	dhLoading DataHeader

	sri	zodb.StorageRecordInformation // ptr to this will be returned by NextData
	dataBuf	[]byte
}

// NextTxn iterates to next/previous transaction record according to iteration direction
func (zi *zIter) NextTxn() (*zodb.TxnInfo, zodb.IStorageRecordIterator, error) {
	switch {
	case zi.Flags & iterEOF != 0:
		//println("already eof")
		return nil, nil, io.EOF

	// XXX needed?
	case zi.Flags & iterPreloaded != 0:
		// first element is already there - preloaded by who initialized TxnIter
		zi.Flags &= ^iterPreloaded
		//fmt.Println("preloaded:", zi.Txnh.Tid)

	default:
		err := zi.iter.NextTxn(LoadAll)
		// XXX EOF ^^^ is not expected (range pre-cut to valid tids) ?
		if err != nil {
			return nil, nil, err
		}
	}

	// XXX how to make sure last good txnh is preserved?
	if (zi.Flags&iterDir != 0 && zi.iter.Txnh.Tid > zi.TidStop) ||
	   (zi.Flags&iterDir == 0 && zi.iter.Txnh.Tid < zi.TidStop) {
		//println("-> EOF")
		zi.Flags |= iterEOF
		return nil, nil, io.EOF
	}

	return &zi.iter.Txnh.TxnInfo, zi, nil
}

// NextData iterates to next data record and loads data content
func (zi *zIter) NextData() (*zodb.StorageRecordInformation, error) {
	err := zi.iter.NextData()
	if err != nil {
		return nil, err	// XXX recheck
	}

	zi.sri.Oid = zi.iter.Datah.Oid
	zi.sri.Tid = zi.iter.Datah.Tid

	// NOTE dh.LoadData() changes dh state while going through backpointers -
	// - need to use separate dh because of this
	zi.dhLoading = zi.iter.Datah
	zi.sri.Data = zi.dataBuf
	err = zi.dhLoading.LoadData(zi.iter.fsSeq, &zi.sri.Data)
	if err != nil {
		return nil, err	// XXX recheck
	}

	// if memory was reallocated - use it next time
	if cap(zi.sri.Data) > cap(zi.dataBuf) {
		zi.dataBuf = zi.sri.Data
	}

	zi.sri.DataTid = zi.dhLoading.Tid
	return &zi.sri, nil
}




// iterStartError is the iterator created when there are preparatory errors
// this way we offload clients, besides handling NextTxn errors, from also
// handling error cases from Iterate.
//
// XXX bad idea? (e.g. it will prevent from devirtualizing what Iterate returns)
type iterStartError struct {
	err error
}

func (e *iterStartError) NextTxn(*zodb.TxnInfo, zodb.IStorageIterator, error) {
	return nil, nil, e.err
}

func (fs *FileStorage) Iterate(tidMin, tidMax zodb.Tid) zodb.IStorageIterator {
	//fmt.Printf("iterate %v..%v\n", tidMin, tidMax)
	// FIXME case when only 0 or 1 txn present
	if tidMin < fs.txnhMin.Tid {
		tidMin = fs.txnhMin.Tid
	}
	if tidMax > fs.txnhMax.Tid {
		tidMax = fs.txnhMax.Tid
	}

	// XXX naming	-> ziter?
	ziter := &zIter{iter}

	// when iterating use IO optimized for sequential access
	// XXX -> IterateRaw
	fsSeq := xbufio.NewSeqReaderAt(fs.file)
	iter.txnIter.fsSeq = fsSeq
	iter.dataIter.fsSeq = fsSeq
	iter.dataIter.Txnh = &iter.txnIter.Txnh

	if tidMin > tidMax {
		Iter.txnIter.Flags |= iterEOF	// empty
		return &Iter
	}

	// scan either from file start or end, depending which way it is likely closer, to tidMin
	// XXX put iter into ptr to Iter ^^^
	iter := &Iter.txnIter

	if (tidMin - fs.txnhMin.Tid) < (fs.txnhMax.Tid - tidMin) {
		//fmt.Printf("forward %.1f%%\n", 100 * float64(tidMin - fs.txnhMin.Tid) / float64(fs.txnhMax.Tid - fs.txnhMin.Tid))
		iter.Flags = 1*iterDir | iterPreloaded
		iter.Txnh.CloneFrom(&fs.txnhMin)
		iter.TidStop = tidMin - 1	// XXX overflow
	} else {
		//fmt.Printf("backward %.1f%%\n", 100 * float64(tidMin - fs.txnhMin.Tid) / float64(fs.txnhMax.Tid - fs.txnhMin.Tid))
		iter.Flags = 0*iterDir | iterPreloaded
		iter.Txnh.CloneFrom(&fs.txnhMax)
		iter.TidStop = tidMin
	}

	// XXX recheck how we enter loop
	var err error
	for {
		err = iter.NextTxn(LoadNoStrings)
		if err != nil {
			err = okEOF(err)
			break
		}
	}

	if err != nil {
		return &iterStartError{err}	// XXX err ctx
	}

	//fmt.Printf("tidRange: %v..%v -> found %v @%v\n", tidMin, tidMax, iter.Txnh.Tid, iter.Txnh.Pos)

	// where to start around tidMin found - let's reinitialize iter to
	// iterate appropriately forward up to tidMax
	iter.Flags &= ^iterEOF
	if iter.Flags&iterDir != 0 {
		// when ^^^ we were searching forward first txn was already found
		err = iter.Txnh.loadStrings(fsSeq)	// XXX ok?	XXX -> move NextTxn() ?
		if err != nil {
			return &iterStartError{err}
		}
		iter.Flags |= iterPreloaded
	}
	iter.Flags |= iterDir
	iter.TidStop = tidMax

	return &Iter
}

// computeIndex builds new in-memory index for FileStorage
// XXX naming
func (fs *FileStorage) computeIndex(ctx context.Context) (index *Index, err error) {
	// TODO err ctx <file>: <reindex>:

	index = IndexNew()
	index.TopPos = txnValidFrom

	// similar to Iterate but we know we start from the beginning and do
	// not want to load actual data - only data headers.
	fsSeq := xbufio.NewSeqReaderAt(fs.file)

	// pre-setup txnh so that txnh.LoadNext starts loading from the beginning of file
	txnh := &TxnHeader{Pos: index.TopPos, Len: -2}	// XXX -2
	dh   := &DataHeader{}

loop:
	for {
		// check ctx cancel once per transaction
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// XXX merge logic into LoadNext/LoadPrev
		switch txnh.Len {
		case -2:
			err = txnh.Load(fsSeq, txnh.Pos, LoadNoStrings)
		default:
			err = txnh.LoadNext(fsSeq, LoadNoStrings)
		}

		if err != nil {
			err = okEOF(err)
			break
		}

		// XXX check txnh.Status != TxnInprogress

		index.TopPos = txnh.Pos + txnh.Len

		// first data iteration will go to first data record
		dh.Pos = txnh.DataPos()
		dh.DataLen = -DataHeaderSize

		for {
			err = dh.LoadNext(fsSeq, txnh)
			if err != nil {
				err = okEOF(err)
				if err != nil {
					break loop
				}
				break
			}

			index.Set(dh.Oid, dh.Pos)
		}
	}

	if err != nil {
		return nil, err
	}
	return index, nil
}

// loadIndex loads on-disk index to RAM
func (fs *FileStorage) loadIndex() error {
	// XXX lock?

	index, err := LoadIndexFile(fs.file.Name() + ".index")
	if err != nil {
		return err	// XXX err ctx
	}

	// XXX here?
	// TODO verify index sane / topPos matches
	if index.TopPos != fs.txnhMax.Pos + fs.txnhMax.Len {
		panic("inconsistent index topPos")	// XXX
	}

	fs.index = index
	return nil
}

// saveIndex flushes in-RAM index to disk
func (fs *FileStorage) saveIndex() error {
	// XXX lock?

	err := fs.index.SaveFile(fs.file.Name() + ".index")
	return err
}

// IndexCorruptError is the error returned when index verification fails
// XXX but io errors during verification return not this
type IndexCorruptError struct {
	index   *Index
	indexOk *Index
}

func (e *IndexCorruptError) Error() string {
	// TODO show delta ?
	return "index corrupt"
}

// VerifyIndex verifies that index is correct
// XXX -> not exported @ fs1
func (fs *FileStorage) VerifyIndex(ctx context.Context) error {
	// XXX lock appends?

	// XXX if .index is not yet loaded - load it

	indexOk, err := fs.computeIndex(ctx)
	if err != nil {
		return err	// XXX err ctx
	}

	if !indexOk.Equal(fs.index) {
		err = &IndexCorruptError{index: fs.index, indexOk: indexOk}
	}

	return err
}


// Reindex rebuilds the index
// XXX -> not exported @ fs1
func (fs *FileStorage) Reindex(ctx context.Context) error {
	// XXX lock appends?

	index, err := fs.computeIndex(ctx)
	if err != nil {
		return err
	}

	fs.index = index

	err = fs.saveIndex()
	return err	// XXX ok?
}
