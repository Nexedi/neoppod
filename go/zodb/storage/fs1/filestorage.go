// Copyright (C) 2017-2018  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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
//	- transaction record header represented by TxnHeader,
//	- several data records corresponding to modified objects,
//	- redundant transaction length at the end of transaction record.
//
// Every data record consists of:
//
//	- data record header represented by DataHeader,
//	- actual data following the header.
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
// Unless one is doing something FileStorage-specific, it is advised not to use
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
	"time"

	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/go123/xerr"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
)

// FileStorage is a ZODB storage which stores data in simple append-only file
// organized as transactional log.
//
// It is on-disk compatible with FileStorage from ZODB/py.
type FileStorage struct {
	file  *os.File

	// protects updates to index and to txnh{Min,Max} - in other words
	// to everything that depends on what current last transaction is.
	mu sync.RWMutex

	index *Index // oid -> data record position in transaction which last changed oid

	// transaction headers for min/max transactions committed
	// (both with .Len=0 & .Tid=0 if database is empty)
	txnhMin TxnHeader
	txnhMax TxnHeader

	// driver client <- watcher: data file updates.
	watchq chan watchEvent
}

// IStorageDriver
var _ zodb.IStorageDriver = (*FileStorage)(nil)

func (fs *FileStorage) LastTid(_ context.Context) (zodb.Tid, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	return fs.txnhMax.Tid, nil // txnhMax.Tid = 0, if empty
}

func (fs *FileStorage) LastOid(_ context.Context) (zodb.Oid, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	lastOid, _ := fs.index.Last() // returns zero-value, if empty
	return lastOid, nil
}

func (fs *FileStorage) URL() string {
	return fs.file.Name()
}

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


func (fs *FileStorage) Load(_ context.Context, xid zodb.Xid) (buf *mem.Buf, serial zodb.Tid, err error) {
	// FIXME zodb.TidMax is only 7fff... tid from outside can be ffff...
	// -> TODO reject tid out of range

	// FIXME kill Load_XXXWithNextSerialXXX after neo/py cache does not depend on next_serial
	buf, serial, _, err = fs.Load_XXXWithNextSerialXXX(nil, xid)
	return buf, serial, err
}

// XXX temporary function - will go away:
//
// FIXME kill Load_XXXWithNextSerialXXX after neo/py cache does not depend on next_serial
func (fs *FileStorage) Load_XXXWithNextSerialXXX(_ context.Context, xid zodb.Xid) (buf *mem.Buf, serial, nextSerial zodb.Tid, err error) {
	buf, serial, nextSerial, err = fs.load(xid)
	if err != nil {
		err = &zodb.OpError{URL: fs.URL(), Op: "load", Args: xid, Err: err}
	}
	return buf, serial, nextSerial, err
}


// FIXME kill nextSerial support after neo/py cache does not depend on next_serial
func (fs *FileStorage) load(xid zodb.Xid) (buf *mem.Buf, serial, nextSerial zodb.Tid, err error) {
	// lookup in index position of oid data record within latest transaction which changed this oid
	fs.mu.RLock()
	dataPos, ok := fs.index.Get(xid.Oid)
	fs.mu.RUnlock()
	if !ok {
		return nil, 0, 0, &zodb.NoObjectError{Oid: xid.Oid}
	}

	// XXX go compiler cannot deduce dh should be on stack here
	//dh := DataHeader{Oid: xid.Oid, Tid: zodb.TidMax, PrevRevPos: dataPos}
	dh := DataHeaderAlloc()
	dh.Oid = xid.Oid
	dh.Tid = zodb.TidMax
	dh.PrevRevPos = dataPos
	//defer dh.Free()
	buf, serial, nextSerial, err = fs._load(dh, xid)
	dh.Free()
	return buf, serial, nextSerial, err
}

// FIXME kill nextSerial support after neo/py cache does not depend on next_serial
func (fs *FileStorage) _load(dh *DataHeader, xid zodb.Xid) (*mem.Buf, zodb.Tid, zodb.Tid, error) {
	// search backwards for when we first have data record with tid satisfying xid.At
	var nextSerial zodb.Tid
	for {
		nextSerial = dh.Tid
		err := dh.LoadPrevRev(fs.file)
		if err != nil {
			if err == io.EOF {
				// object was created after xid.At
				err = &zodb.NoDataError{Oid: xid.Oid, DeletedAt: 0}
			}

			return nil, 0, 0, err
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
		return nil, 0, 0, err
	}
	if buf.Data == nil {
		// object was deleted
		return nil, 0, 0, &zodb.NoDataError{Oid: xid.Oid, DeletedAt: serial}
	}

	return buf, serial, nextSerial, nil
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

	datai   zodb.DataInfo // ptr to this will be returned by .NextData
	dataBuf *mem.Buf
}

type zIterFlags int
const (
	zIterEOF       zIterFlags = 1 << iota // EOF reached
	zIterPreloaded                        // data for this iteration was already preloaded
)

// NextTxn iterates to next/previous transaction record according to iteration direction.
func (zi *zIter) NextTxn(_ context.Context) (*zodb.TxnInfo, zodb.IDataIterator, error) {
	// TODO err -> OpError("iter", tidmin..tidmax)
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

// NextData iterates to next data record and loads data content.
func (zi *zIter) NextData(_ context.Context) (*zodb.DataInfo, error) {
	// TODO err -> OpError("iter", tidmin..tidmax)
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
	fs.mu.RLock()

	// check for empty database
	if fs.txnhMin.Len == 0 {
		// empty database - no such record
		fs.mu.RUnlock()
		return TxnHeader{}, io.EOF
	}

	// now we know the database is not empty and thus txnh min & max are valid
	// clone them to unalias strings memory
	var tmin, tmax TxnHeader
	tmin.CloneFrom(&fs.txnhMin)
	tmax.CloneFrom(&fs.txnhMax)

	fs.mu.RUnlock()

	if tmax.Tid < tid {
		return TxnHeader{}, io.EOF // no such record
	}
	if tmin.Tid >= tid {
		return tmin, nil // tmin satisfies
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

// Iterate creates zodb-level iterator for tidMin..tidMax range.
func (fs *FileStorage) Iterate(_ context.Context, tidMin, tidMax zodb.Tid) zodb.ITxnIterator {
	// when iterating use IO optimized for sequential access
	fsSeq := seqReadAt(fs.file)
	ziter := &zIter{iter: Iter{R: fsSeq}}
	iter := &ziter.iter

	// find first txn : txn.tid >= tidMin
	txnh, err := fs.findTxnRecord(fsSeq, tidMin)
	switch {
	case err == io.EOF:
		ziter.zFlags |= zIterEOF // empty
		return ziter

	case err != nil:
		return &iterStartError{&zodb.OpError{
			URL:  fs.URL(),
			Op:   "iter",
			// XXX (?) add TidRange type which prints as
			// "tidmin..tidmax" with omitting ends if it is either 0 or ∞
			Args: []zodb.Tid{tidMin, tidMax},
			Err:  err,
		}}
	}

	// setup iter from what findTxnRecord found
	iter.Txnh = txnh
	iter.Datah.Pos = txnh.DataPos()      // XXX dup wrt Iter.NextTxn
	iter.Datah.DataLen = -DataHeaderSize // first iteration will go to first data record

	iter.Dir = IterForward // TODO allow both ways iteration at ZODB level

	ziter.zFlags |= zIterPreloaded
	ziter.tidStop = tidMax

	return ziter
}

// --- watcher ---

// watcher watches updates to .file and notifies Watch about new transactions.
//
// watcher is the only place that mutates index and txnh{Min,Max}.
// XXX ^^^ will change after commit is implemented.
func (fs *FileStorage) watcher(w *fsnotify.Watcher) {
	defer w.Close() // XXX lclose
	err := fs._watcher(w)
	// XXX fs.watchErr = err  (-> fail other operations)
	_ = err
	log.Print(err)
}

func (fs *FileStorage) _watcher(w *fsnotify.Watcher) (err error) {
	f := fs.file
	idx := fs.index
	defer xerr.Contextf(&err, "%s: watcher", f.Name())

	// loop checking f.size vs topPos
	//
	// besides relying on notify we also check file periodically to avoid
	// stalls due to e.g. OS notification errors.
	tick := time.NewTicker(1*time.Second)
	defer tick.Stop()
	var t0partial time.Time
	first := true
mainloop:
	for {
		if !first {
			//tracef("select ...")
			select {
			// XXX handle close

			case err := <-w.Errors:
				//tracef("error: %s", err)
				if err != fsnotify.ErrEventOverflow {
					return err
				}
				// events lost, but it is safe since we are always rechecking file size

			case <-w.Events:
				// we got some kind of "file was modified" event (e.g.
				// write, truncate, chown ...) -> it is time to check the file again.
				//tracef("event: %s", e)

			case <-tick.C:
				// recheck the file periodically.
				//tracef("tick")
			}
		}
		first = false

		// check f size, to see whether there could be any updates.
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		fsize := fi.Size()
		tracef("toppos: %d\tfsize: %d\n", idx.TopPos, fsize)
		switch {
		case fsize == idx.TopPos:
			continue // same as before
		case fsize < idx.TopPos:
			// XXX add pack support
			return fmt.Errorf("file truncated (%d -> %d)", idx.TopPos, fsize)
		}

		// there is some data after toppos - try to advance as much as we can.
		// start iterating afresh with empty buffer.
		//tracef("scanning ...")
		it := Iterate(seqReadAt(f), idx.TopPos, IterForward)
		for {
			err = it.NextTxn(LoadNoStrings)
			if err != nil {
				// transaction header could not be fully read.
				//
				// Even though FileStorage code always calls write with full txn
				// header, the kernel could do the write in parts, e.g. if written
				// region overlaps page boundary.
				//
				// we check for some time to distinguish in-progress write from just
				// trailing garbage.
				if errors.Cause(err) == io.ErrUnexpectedEOF {
					now := time.Now()
					if t0partial.IsZero() {
						t0partial = now
					} else if now.Sub(t0partial) > 3*time.Second {
						return err // garbage
					}
				} else {
					// not ok - e.g. IO error
					// only EOF is ok - it can happen when transaction was aborted
					if err != io.EOF {
						return err
					}

					// EOF - reset t₀(partial)
					t0partial = time.Time{}
				}

				// after any error (EOF, partial read) we have to resync
				continue mainloop
			}

			// read ok - reset t₀(partial)
			t0partial = time.Time{}

			tracef("@%d tid=%s st=%q", it.Txnh.Pos, it.Txnh.Tid, it.Txnh.Status)

			// XXX dup wrt Index.Update

			// we could successfully read the transaction header. Try to see now,
			// whether it is finished transaction or not.
			if it.Txnh.Status == zodb.TxnInprogress {
				// not yet. we have to resync because transaction finish writes
				// to what we have already buffered.
				continue mainloop
			}

			// it is fully-committed transaction. scan its data records to update
			// our index & notify watchers. There is no expected errors here.
			var oidv []zodb.Oid
			update := map[zodb.Oid]int64{}
			for {
				err = it.NextData()
				if err != nil {
					err = okEOF(err)
					if err != nil {
						return err
					}
					break
				}

				update[it.Datah.Oid] = it.Datah.Pos
				oidv = append(oidv, it.Datah.Oid)
			}

			// update index & txnh{MinMax}
			fs.mu.Lock()
			idx.TopPos = it.Txnh.Pos + it.Txnh.Len
			for oid, pos := range update {
				idx.Set(oid, pos)
			}
			fs.txnhMax.CloneFrom(&it.Txnh)
			if fs.txnhMin.Len == 0 { // was empty
				fs.txnhMin.CloneFrom(&it.Txnh)
			}
			fs.mu.Unlock()

			tracef("-> tid=%s  δoidv=%v", it.Txnh.Tid, oidv) // XXX oidv=[0,0] - recheck

			// XXX cancel on close
			fs.watchq <- watchEvent{it.Txnh.Tid, oidv}
			//tracef("zzz")
		}
	}
}

// watchEvent is one event from watch to Watch
type watchEvent struct {
	tid  zodb.Tid
	oidv []zodb.Oid
}

func (fs *FileStorage) Watch(ctx context.Context) (_ zodb.Tid, _ []zodb.Oid, err error) {
	defer xerr.Contextf(&err, "%s: watch", fs.file.Name())

	// XXX handle close
	//tracef("watch -> select ...")
	select {
	case <-ctx.Done():
		//tracef("\t-> canceled")
		return zodb.InvalidTid, nil, ctx.Err()

	case w := <-fs.watchq:
		//tracef("\t-> data")
		return w.tid, w.oidv, nil
	}
}

// --- open + rebuild index ---

func (fs *FileStorage) Close() error {
	// XXX stop watcher

	err := fs.file.Close()
	if err != nil {
		return &zodb.OpError{URL: fs.URL(), Op: "close", Args: nil, Err: err}
	}

	// TODO if opened !ro -> .saveIndex()

	return nil
}

// Open opens FileStorage @path.
//
// TODO read-write support
func Open(ctx context.Context, path string) (_ *FileStorage, err error) {
	fs := &FileStorage{
		watchq: make(chan watchEvent),
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fs.file = f
	defer func() {
		if err != nil {
			f.Close() // XXX -> lclose
		}
	}()

	// XXX wrap err with "open <path>" ?

	// check file magic
	fh := FileHeader{}
	err = fh.Load(f)
	if err != nil {
		return nil, err
	}

	// load index
	fseq := seqReadAt(f)
	index, err := LoadIndexFile(f.Name() + ".index")
	if err == nil {
		// index exists & loaded ok - quickly verify its sanity for last 100 transactions
		_, err = index.Verify(ctx, fseq, 100, nil/*no progress*/)
		if err != nil {
			index = nil // not sane - we will rebuild
		}
	}
	if err != nil {
		// index either did not exist, or corrupt or IO error - rebuild it from scratch
		log.Print(err)
		log.Printf("%s: index rebuild...", path)
		index, err = BuildIndex(ctx, fseq, nil/*no progress; XXX log it? */)
		if err != nil {
			return nil, err
		}
	}

	// index loaded. In particular this gives us index.TopPos that is, possibly
	// outdated, valid position for start of a transaction in the data file.
	// Update the index starting from that till latest transaction.
	err = index.Update(ctx, fseq, -1, nil/*no progress; XXX log it? */)
	if err != nil {
		return nil, err
	}

	fs.index = index

	// now we have the index covering till last transaction in data file.
	// fill-in min/max txnh
	if index.TopPos > txnValidFrom {
		err = fs.txnhMin.Load(f, txnValidFrom, LoadAll)
		err = noEOF(err)
		if err != nil {
			return nil, err
		}

		_ = fs.txnhMax.Load(f, index.TopPos, LoadAll)
		// NOTE it will be EOF on stable storage, but it might be non-EOF
		// if a txn-in-progress was committed only partially. We care only
		// that we read .LenPrev ok.
		switch fs.txnhMax.LenPrev {
		case -1:
			return nil, fmt.Errorf("%s: could not read LenPrev @%d (last transaction)", f.Name(), fs.txnhMax.Pos)
		case 0:
			return nil, fmt.Errorf("%s: could not read LenPrev @%d (last transaction): unexpected EOF backward", f.Name(), fs.txnhMax.Pos)

		default:
			// .LenPrev is ok - read last previous record
			err = fs.txnhMax.LoadPrev(f, LoadAll)
			if err != nil {
				return nil, err
			}
		}
	}

	// there might be simultaneous updates to the data file from outside.
	// launch the watcher who will observe them.
	//
	// the filesystem watcher is setup before fs returned to user to avoid
	// race of missing early file writes.
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			w.Close()	// XXX lclose
		}
	}()

	err = w.Add(f.Name())
	if err != nil {
		return nil, err
	}

	go fs.watcher(w)
	return fs, nil
}
