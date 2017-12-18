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

package fs1
// records definition + basic operations on them

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/xcommon/xbufio"
	"lab.nexedi.com/kirr/neo/go/xcommon/xio"

	"lab.nexedi.com/kirr/go123/xbytes"
)

// FileHeader represents header of whole data file
type FileHeader struct {
	Magic [4]byte
}

// TxnHeader represents header of a transaction record
type TxnHeader struct {
	Pos	int64	// position of transaction start
	LenPrev	int64	// whole previous transaction record length (see EOF/error rules in Load)
	Len	int64	// whole transaction record length          (see EOF/error rules in Load)

	// transaction metadata itself
	zodb.TxnInfo

	// underlying memory for header loading and for user/desc/extension strings
	// invariant: after successful TxnHeader load len(.workMem) = lenUser + lenDesc + lenExt
	//            as specified by on-disk header.
	workMem	[]byte
}

// DataHeader represents header of a data record
type DataHeader struct {
	Pos		int64	// position of data record start
	Oid             zodb.Oid
	Tid             zodb.Tid
	PrevRevPos	int64	// position of this oid's previous-revision data record
	TxnPos          int64	// position of transaction record this data record belongs to
	//_		uint16	// 2-bytes with zero values. (Was version length.)
	DataLen		int64	// length of following data. if 0 -> following = 8 bytes backpointer
				// if backpointer == 0 -> oid deleted

	// underlying memory for header loading (to avoid allocations)
	workMem [DataHeaderSize]byte
}

const (
	Magic = "FS21"	// every FileStorage file starts with this

	// on-disk sizes
	FileHeaderSize		= 4
	TxnHeaderFixSize	= 8+8+1+2+2+2		// without user/desc/ext strings
	txnXHeaderFixSize	= 8 + TxnHeaderFixSize	// ^^^ with trail LenPrev from previous record
	DataHeaderSize		= 8+8+8+8+2+8

	// txn/data pos that if < vvv are for sure invalid
	txnValidFrom	= FileHeaderSize
	dataValidFrom	= txnValidFrom + TxnHeaderFixSize

	// invalid length that indicates start of iteration for TxnHeader LoadNext/LoadPrev
	// NOTE load routines check loaded fields to be valid and .Len in particular,
	//      so it can never come to us from outside to be this.
	lenIterStart int64 = -0x1111111111111112	// = 0xeeeeeeeeeeeeeeee if unsigned
)

// RecordError represents error associated with operation on a record in
// FileStorage data file.
type RecordError struct {
	Path	string	// path of the data file
	Record	string	// record kind - "file header", "transaction record", "data record", ...
	Pos	int64	// position of record
	Subj	string	// subject context for the error - e.g. "read" or "check"
	Err	error	// actual error
}

func (e *RecordError) Cause() error {
	return e.Err
}

func (e *RecordError) Error() string {
	// XXX omit path: when .Err already contains it (e.g. when it is os.PathError)?
	return fmt.Sprintf("%s: %s @%d: %s: %s", e.Path, e.Record, e.Pos, e.Subj, e.Err)
}

// err creates RecordError for transaction located at txnh.Pos
func (txnh *TxnHeader) err(r io.ReaderAt, subj string, err error) error {
	return &RecordError{xio.Name(r), "transaction record", txnh.Pos, subj, err}
}


// err creates RecordError for data record located at dh.Pos
func (dh *DataHeader) err(r io.ReaderAt, subj string, err error) error {
	return &RecordError{xio.Name(r), "data record", dh.Pos, subj, err}
}


// ierr is an interface for something which can create errors.
// it is used by TxnHeader and DataHeader to create appropriate errors with their context.
type ierr interface {
	err(r io.ReaderAt, subj string, err error) error
}

// errf is syntactic shortcut for err and fmt.Errorf
func errf(r io.ReaderAt, e ierr, subj, format string, a ...interface{}) error {
	return e.err(r, subj, fmt.Errorf(format, a...))
}

// checkErr is syntactic shortcut for errf("check", ...)
func checkErr(r io.ReaderAt, e ierr, format string, a ...interface{}) error {
	return errf(r, e, "check", format, a...)
}

// bug panics with errf("bug", ...)
func bug(r io.ReaderAt, e ierr, format string, a ...interface{}) {
	panic(errf(r, e, "bug", format, a...))
}


// --- File header ---

// Load reads and decodes file header.
func (fh *FileHeader) Load(r io.ReaderAt) error {
	_, err := r.ReadAt(fh.Magic[:], 0)
	err = okEOF(err)
	if err != nil {
		return  err
	}
	if string(fh.Magic[:]) != Magic {
		return fmt.Errorf("%s: invalid fs1 magic %q", xio.Name(r), fh.Magic)
	}

	return nil
}

// --- Transaction record ---

// HeaderLen returns whole transaction header length including its variable part.
//
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

	luser := cap(txnh2.User)
	xdesc := luser + cap(txnh2.Description)
	xext  := xdesc + cap(txnh2.Extension)
	txnh.User	 = workMem[0:0:luser]		[:len(txnh2.User)]
	txnh.Description = workMem[luser:luser:xdesc]	[:len(txnh2.Description)]
	txnh.Extension   = workMem[xdesc:xdesc:xext]	[:len(txnh2.Extension)]
}

// flags for TxnHeader.Load
type TxnLoadFlags int
const (
	LoadAll		TxnLoadFlags	= 0x00 // load whole transaction header
	LoadNoStrings			= 0x01 // do not load user/desc/ext strings
)

// Load reads and decodes transaction record header @ pos.
//
// Both transaction header starting at pos, and redundant length of previous
// transaction are loaded. The data read is verified for consistency lightly.
//
// No prerequisite requirements are made to previous txnh state.
//
// Rules for .Len/.LenPrev returns:
//
//	.Len = -1	transaction header could not be read
//	.Len = 0	EOF forward
//	.Len > 0 	transaction was read normally
//
//	.LenPrev = -1	prev record length could not be read
//	.LenPrev = 0	EOF backward
//	.LenPrev > 0	LenPrev was read/checked normally
//
// For example when pos points to the end of file .Len will be returned = -1, but
// .LenPrev will be usually valid if file has at least 1 transaction.
func (txnh *TxnHeader) Load(r io.ReaderAt, pos int64, flags TxnLoadFlags) error {
	if cap(txnh.workMem) < txnXHeaderFixSize {
		txnh.workMem = make([]byte, txnXHeaderFixSize, 256 /* to later avoid allocation for typical strings */)
	}
	work := txnh.workMem[:txnXHeaderFixSize]

	txnh.Pos = pos
	txnh.Len = -1		// read error
	txnh.LenPrev = -1	// read error

	if pos < txnValidFrom {
		bug(r, txnh, "Load() on invalid position")
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
				return checkErr(r, txnh, "invalid prev record length: %v", lenPrev)
			}
			posPrev := txnh.Pos - lenPrev
			if posPrev < txnValidFrom {
				return checkErr(r, txnh, "prev record length goes beyond valid area: %v", lenPrev)
			}
			if posPrev < txnValidFrom + TxnHeaderFixSize && posPrev != txnValidFrom {
				return checkErr(r, txnh, "prev record does not land exactly at valid area start: %v", posPrev)
			}
			txnh.LenPrev = lenPrev
		}
	} else {
		// read only current txn without previous record length
		n, err = r.ReadAt(work[8:], pos)
		txnh.LenPrev = 0	// EOF backward
	}


	if err != nil {
		if err == io.EOF && n == 0 {
			txnh.Len = 0	// EOF forward
			return err	// end of stream
		}

		// EOF after txn header is not good - because at least
		// redundant length should be also there
		return txnh.err(r, "read", noEOF(err))
	}

	txnh.Tid = zodb.Tid(binary.BigEndian.Uint64(work[8+0:]))
	if !txnh.Tid.Valid() {
		return checkErr(r, txnh, "invalid tid: %v", txnh.Tid)
	}

	tlen := 8 + int64(binary.BigEndian.Uint64(work[8+8:]))
	if tlen < TxnHeaderFixSize {
		return checkErr(r, txnh, "invalid txn record length: %v", tlen)
	}
	// NOTE no need to check tlen does not go beyon file size - loadStrings/LoadData or LoadNext will catch it.
	// txnh.Len will be set =tlen at last - after checking other fields for correctness.

	txnh.Status = zodb.TxnStatus(work[8+16])
	if !txnh.Status.Valid() {
		return checkErr(r, txnh, "invalid status: %q", txnh.Status)
	}


        luser := binary.BigEndian.Uint16(work[8+17:])
	ldesc := binary.BigEndian.Uint16(work[8+19:])
	lext  := binary.BigEndian.Uint16(work[8+21:])

	lstr := int(luser) + int(ldesc) + int(lext)
	if TxnHeaderFixSize + int64(lstr) + 8 > tlen {
		return checkErr(r, txnh, "strings overlap with txn boundary: %v / %v", lstr, tlen)
	}

	// set .Len at last after doing all header checks
	// this way we make sure we never return bad fixed TxnHeader with non-error .Len
	txnh.Len = tlen

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
		return txnh.err(r, "read strings", noEOF(err))
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
// prerequisites:
//
//	- txnh .Pos, .LenPrev are initialized
//	- optionally if .Len is initialized and txnh was loaded - tid↓ will be also checked
func (txnh *TxnHeader) LoadPrev(r io.ReaderAt, flags TxnLoadFlags) error {
	lenPrev := txnh.LenPrev
	switch lenPrev {
	case -1:
		bug(r, txnh, "LoadPrev() when .LenPrev == error")
	case 0:
		return io.EOF

	case lenIterStart:
		// start of iteration backward:
		// read LenPrev @pos, then tail to LoadPrev
		pos := txnh.Pos
		err := txnh.Load(r, pos, flags)
		if txnh.LenPrev == -1 {
			if err == nil {
				panic("nil err with txnh.LenPrev = error")
			}
			return err
		}
		// we do not care if it was error above as long as txnh.LenPrev could be read.
		return txnh.LoadPrev(r, flags)
	}

	lenCur := txnh.Len
	tidCur := txnh.Tid

	// here we know: Load already checked txnh.Pos - lenPrev to be valid position
	err := txnh.Load(r, txnh.Pos - lenPrev, flags)
	if err != nil {
		// EOF forward is unexpected here
		if err == io.EOF {
			err = txnh.err(r, "read", io.ErrUnexpectedEOF)
		}
		return err
	}

	if txnh.Len != lenPrev {
		return checkErr(r, txnh, "head/tail lengths mismatch: %v, %v", txnh.Len, lenPrev)
	}

	// check tid↓ if we had txnh for "cur" loaded
	if lenCur > 0 && txnh.Tid >= tidCur {
		return checkErr(r, txnh, "tid monitonity broken: %v  ; next: %v", txnh.Tid, tidCur)
	}

	return nil
}

// LoadNext reads and decodes next transaction record header.
//
// prerequisite: txnh .Pos, .Len and .Tid should be already initialized.
func (txnh *TxnHeader) LoadNext(r io.ReaderAt, flags TxnLoadFlags) error {
	lenCur := txnh.Len
	posCur := txnh.Pos
	switch lenCur {
	case -1:
		bug(r, txnh, "LoadNext() when .Len == error")
	case 0:
		return io.EOF

	case lenIterStart:
		// start of iteration forward
		return txnh.Load(r, posCur, flags)
	}

	// valid .Len means txnh was read ok
	tidCur := txnh.Tid

	err := txnh.Load(r, txnh.Pos + lenCur, flags)

	// before checking loading error for next txn, let's first check redundant length
	// NOTE also: err could be EOF
	if txnh.LenPrev >= 0 && txnh.LenPrev != lenCur {
		t := &TxnHeader{Pos: posCur} // txn for which we discovered problem
		return checkErr(r, t, "head/tail lengths mismatch: %v, %v", lenCur, txnh.LenPrev)
	}

	if err != nil {
		return err
	}

	// check tid↑
	if txnh.Tid <= tidCur {
		return checkErr(r, txnh, "tid↑ broken: %v  ; prev: %v", txnh.Tid, tidCur)
	}

	return nil
}

// --- Data record ---

// Len returns whole data record length.
func (dh *DataHeader) Len() int64 {
	dataLen := dh.DataLen
	if dataLen == 0 {
		dataLen = 8 // back-pointer | oid removal
	}

	return DataHeaderSize + dataLen
}


// Load reads and decodes data record header @ pos.
//
// Only the data record header is loaded, not data itself.
// See LoadData for actually loading record's data.
//
// No prerequisite requirements are made to previous dh state.
func (dh *DataHeader) Load(r io.ReaderAt, pos int64) error {
	dh.Pos = -1	// ~ error

	if pos < dataValidFrom {
		bug(r, dh, "Load() on invalid position")
	}

	_, err := r.ReadAt(dh.workMem[:], pos)
	if err != nil {
		return dh.err(r, "read", noEOF(err))
	}

	// XXX also check oid.Valid() ?
	dh.Oid = zodb.Oid(binary.BigEndian.Uint64(dh.workMem[0:]))	// XXX -> zodb.Oid.Decode() ?
	dh.Tid = zodb.Tid(binary.BigEndian.Uint64(dh.workMem[8:]))	// XXX -> zodb.Tid.Decode() ?
	if !dh.Tid.Valid() {
		return checkErr(r, dh, "invalid tid: %v", dh.Tid)
	}

	dh.PrevRevPos = int64(binary.BigEndian.Uint64(dh.workMem[16:]))
	dh.TxnPos = int64(binary.BigEndian.Uint64(dh.workMem[24:]))
	if dh.TxnPos < txnValidFrom {
		return checkErr(r, dh, "invalid txn position: %v", dh.TxnPos)
	}

	if dh.TxnPos + TxnHeaderFixSize > pos {
		return checkErr(r, dh, "txn position not decreasing: %v", dh.TxnPos)
	}
	if dh.PrevRevPos != 0 {	// zero means there is no previous revision
		if dh.PrevRevPos < dataValidFrom {
			return checkErr(r, dh, "invalid prev revision position: %v", dh.PrevRevPos)
		}
		if dh.PrevRevPos + DataHeaderSize > dh.TxnPos - 8 {
			return checkErr(r, dh, "prev revision position (%v) overlaps with txn (%v)", dh.PrevRevPos, dh.TxnPos)
		}
	}

	verlen := binary.BigEndian.Uint16(dh.workMem[32:])
	if verlen != 0 {
		return checkErr(r, dh, "non-zero version: #%v", verlen)
	}

	dh.DataLen = int64(binary.BigEndian.Uint64(dh.workMem[34:]))
	if dh.DataLen < 0 {
		// XXX also check DataLen < max ?
		return checkErr(r, dh, "invalid data len: %v", dh.DataLen)
	}

	dh.Pos = pos
	return nil
}

// LoadPrevRev reads and decodes previous revision data record header.
//
// Prerequisite: dh .Oid .Tid .PrevRevPos are initialized.
//
// When there is no previous revision: io.EOF is returned.
func (dh *DataHeader) LoadPrevRev(r io.ReaderAt) error {
	if dh.PrevRevPos == 0 {
		return io.EOF	// no more previous revisions
	}

	posCur := dh.Pos

	err := dh.loadPrevRev(r, dh.PrevRevPos)
	if err != nil {
		// data record @...: -> (prev rev): data record @...: ...
		// XXX dup wrt DataHeader.err
		err = &RecordError{xio.Name(r), "data record", posCur, "-> (prev rev)", err}
	}
	return err
}

// worker for LoadPrevRev and LoadBack
func (dh *DataHeader) loadPrevRev(r io.ReaderAt, prevPos int64) error {
	oid := dh.Oid
	tid := dh.Tid

	err := dh.Load(r, prevPos)
	if err != nil {
		return err
	}

	if dh.Oid != oid {
		return checkErr(r, dh, "oid mismatch: %s -> %s", oid, dh.Oid)
	}

	if dh.Tid >= tid {
		return checkErr(r, dh, "tid not ↓: %s -> %s", tid, dh.Tid)
	}

	return nil
}

// LoadBackRef reads data for the data record and decodes it as backpointer reference.
//
// Prerequisite: dh loaded and .LenData == 0 (data record with back-pointer).
func (dh *DataHeader) LoadBackRef(r io.ReaderAt) (backPos int64, err error) {
	if dh.DataLen != 0 {
		bug(r, dh, "LoadBack() on non-backpointer data header")
	}

	_, err = r.ReadAt(dh.workMem[:8], dh.Pos + DataHeaderSize)
	if err != nil {
		return 0, dh.err(r, "read data", noEOF(err))
	}

	backPos = int64(binary.BigEndian.Uint64(dh.workMem[0:]))
	if !(backPos == 0 || backPos >= dataValidFrom) {
		return 0, checkErr(r, dh, "invalid backpointer: %v", backPos)
	}
	if backPos + DataHeaderSize > dh.TxnPos - 8 {
		return 0, checkErr(r, dh, "backpointer (%v) overlaps with txn (%v)", backPos, dh.TxnPos)
	}

	return backPos, nil
}

// LoadBack reads and decodes data header for revision linked via back-pointer.
//
// Prerequisite: dh is loaded and .DataLen == 0.
//
// If link is to zero (means deleted record) io.EOF is returned.
func (dh *DataHeader) LoadBack(r io.ReaderAt) error {
	backPos, err := dh.LoadBackRef(r)
	if err != nil {
		return err
	}

	if backPos == 0 {
		return io.EOF	// oid was deleted
	}

	posCur := dh.Pos

	err = dh.loadPrevRev(r, backPos)
	if err != nil {
		// data record @...: -> (prev rev): data record @...: ...
		// XXX dup wrt DataHeader.err
		err = &RecordError{xio.Name(r), "data record", posCur, "-> (back)", err}
	}

	return err
}

// LoadNext reads and decodes data header for next data record in the same transaction.
//
// Prerequisite: dh .Pos .DataLen are initialized.
//
// When there is no more data records: io.EOF is returned.
func (dh *DataHeader) LoadNext(r io.ReaderAt, txnh *TxnHeader) error {
	err := dh.loadNext(r, txnh)
	if err != nil && err != io.EOF {
		err = txnh.err(r, "-> (iter data)", err)
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
		// XXX dup wrt DataHeader.err
		return &RecordError{xio.Name(r), "data record", nextPos, "check",
			fmt.Errorf("data record header [..., %d] overlaps txn boundary [..., %d)",
				nextPos + DataHeaderSize, txnTailPos)}
	}

	err := dh.Load(r, nextPos)
	if err != nil {
		return err
	}

	if dh.Tid != txnh.Tid {
		return checkErr(r, dh, "tid mismatch: %s -> %s", txnh.Tid, dh.Tid)
	}
	if dh.TxnPos != txnh.Pos {
		return checkErr(r, dh, "txn position not pointing back: %d", dh.TxnPos)
	}
	if dh.Pos + dh.Len() > txnTailPos {
		return checkErr(r, dh, "data record [..., %d) overlaps txn boundary [..., %d)",
			dh.Pos + dh.Len(), txnTailPos)
	}

	return nil
}

// LoadData loads data for the data record taking backpointers into account.
//
// NOTE on success dh state is changed to data header of original data transaction.
// NOTE "deleted" records are indicated via returning buf with .Data=nil without error.
func (dh *DataHeader) LoadData(r io.ReaderAt) (*zodb.Buf, error) {
	// scan via backpointers
	for dh.DataLen == 0 {
		err := dh.LoadBack(r)
		if err != nil {
			if err == io.EOF {
				return &zodb.Buf{Data: nil}, nil // deleted
			}
			return nil, err
		}
	}

	// now read actual data
	buf := zodb.BufAlloc64(dh.DataLen)
	_, err := r.ReadAt(buf.Data, dh.Pos + DataHeaderSize)
	if err != nil {
		buf.Release()
		return nil, dh.err(r, "read data", noEOF(err))
	}

	return buf, nil
}

// --- raw iteration ---

// Iter is combined 2-level iterator over transaction and data records
type Iter struct {
	R	io.ReaderAt
	Dir	IterDir

	Txnh	TxnHeader	// current transaction record information
	Datah	DataHeader	// current data record information
}

type IterDir int
const (
	IterForward	IterDir = iota
	IterBackward
)

// NextTxn iterates to next/previous transaction record according to iteration direction.
//
// The data header is reset to iterate inside transaction record that becomes current.
func (it *Iter) NextTxn(flags TxnLoadFlags) error {
	var err error
	switch it.Dir {
	case IterForward:
		err = it.Txnh.LoadNext(it.R, flags)
	case IterBackward:
		err = it.Txnh.LoadPrev(it.R, flags)
	default:
		panic("Iter.Dir invalid")
	}

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
	return it.Datah.LoadNext(it.R, &it.Txnh)
}


// Iterate creates Iter to iterate over r starting from posStart in direction dir
func Iterate(r io.ReaderAt, posStart int64, dir IterDir) *Iter {
	it := &Iter{R: r, Dir: dir, Txnh: TxnHeader{Pos: posStart}}
	switch dir {
	case IterForward:
		it.Txnh.Len = lenIterStart
	case IterBackward:
		it.Txnh.LenPrev = lenIterStart
	default:
		panic("dir invalid")
	}
	return it
}


// IterateFile opens file @ path read-only and creates Iter to iterate over it.
//
// The iteration will use buffering over os.File optimized for sequential access.
// You are responsible to eventually close the file after the iteration is done.
func IterateFile(path string, dir IterDir) (iter *Iter, file *os.File, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	// close file in case we return with an error
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	// use IO optimized for sequential access when iterating
	fSeq := xbufio.NewSeqReaderAt(f)

	switch dir {
	case IterForward:
		return Iterate(fSeq, txnValidFrom, IterForward), f, nil

	case IterBackward:
		// get file size as topPos and start iterating backward from it
		// XXX half-committed transaction might be there.
		fi, err := f.Stat()
		if err != nil {
			return nil, nil, err
		}
		topPos := fi.Size()

		return Iterate(fSeq, topPos, IterBackward), f, nil

	default:
		panic("dir invalid")
	}
}
