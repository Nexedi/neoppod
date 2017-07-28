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
// index for quickly finding oid -> latest data record

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"os"
	"path/filepath"
	"strconv"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1/fsb"

	pickle "github.com/kisielk/og-rek"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/go123/xerr"

	"lab.nexedi.com/kirr/neo/go/xcommon/xbufio"
	"lab.nexedi.com/kirr/neo/go/xcommon/xio"
)

// Index is in-RAM Oid -> Data record position mapping used to associate Oid
// with Data record in latest transaction which changed it.
type Index struct {
	// this index covers data file up to < .TopPos
	// usually for whole-file index TopPos is position pointing just past
	// the last committed transaction.
	TopPos int64

	*fsb.Tree
}

// IndexNew creates new empty index
func IndexNew() *Index {
	return &Index{TopPos: txnValidFrom, Tree: fsb.TreeNew()}
}

// NOTE Get/Set/... are taken as-is from fsb.Tree


// --- index load/save ---

// on-disk index format
// (changed in 2010 in https://github.com/zopefoundation/ZODB/commit/1bb14faf)
//
// TopPos
// (oid[:6], fsBucket)
// (oid[:6], fsBucket)
// ...
// None
//
//
// fsBucket:
// oid[6:8]oid[6:8]oid[6:8]...pos[2:8]pos[2:8]pos[2:8]...

const (
	oidPrefixMask  zodb.Oid = (1<<64-1) ^ (1<<16 - 1)	// 0xffffffffffff0000
	posInvalidMask uint64   = (1<<64-1) ^ (1<<48 - 1)	// 0xffff000000000000
	posValidMask   uint64   = 1<<48 - 1			// 0x0000ffffffffffff
)

// IndexSaveError is the error type returned by index save routines
type IndexSaveError struct {
	Err error // error that occurred during the operation
}

func (e *IndexSaveError) Error() string {
	return "index save: " + e.Err.Error()
}

// Save saves index to a writer
func (fsi *Index) Save(w io.Writer) error {
	var err error

	{
		p := pickle.NewEncoder(w)

		err = p.Encode(fsi.TopPos)
		if err != nil {
			goto out
		}

		var oidb [8]byte
		var posb [8]byte
		var oidPrefixCur zodb.Oid	// current oid[0:6] with [6:8] = 00
		oidBuf := []byte{}		// current oid[6:8]oid[6:8]...
		posBuf := []byte{}		// current pos[2:8]pos[2:8]...
		var t [2]interface{}		// tuple for (oid, fsBucket.toString())

		e, _ := fsi.SeekFirst()
		if e != nil {
			defer e.Close()

			for  {
				oid, pos, errStop := e.Next()
				oidPrefix := oid & oidPrefixMask

				if oidPrefix != oidPrefixCur || errStop != nil {
					// emit (oid[0:6], oid[6:8]oid[6:8]...pos[2:8]pos[2:8]...)
					binary.BigEndian.PutUint64(oidb[:], uint64(oidPrefixCur))
					t[0] = oidb[0:6]
					t[1] = bytes.Join([][]byte{oidBuf, posBuf}, nil)
					err = p.Encode(pickle.Tuple(t[:]))
					if err != nil {
						goto out
					}

					oidPrefixCur = oidPrefix
					oidBuf = oidBuf[:0]
					posBuf = posBuf[:0]
				}

				if errStop != nil {
					break
				}

				// check pos does not overflow 6 bytes
				if uint64(pos) & posInvalidMask != 0 {
					err = fmt.Errorf("entry position too large: 0x%x", pos)
					goto out
				}

				binary.BigEndian.PutUint64(oidb[:], uint64(oid))
				binary.BigEndian.PutUint64(posb[:], uint64(pos))

				oidBuf = append(oidBuf, oidb[6:8]...)
				posBuf = append(posBuf, posb[2:8]...)
			}
		}

		err = p.Encode(pickle.None{})
	}

out:
	if err == nil {
		return err
	}

	if _, ok := err.(*pickle.TypeError); ok {
		panic(err)	// all our types are expected to be supported by pickle
	}

	// otherwise it is an error returned by writer, which should already
	// have filename & op as context.
	return &IndexSaveError{err}
}

// SaveFile saves index to a file @ path
//
// Index data is first saved to a temporary file and when complete the
// temporary is renamed to be at requested path. This way file @ path will be
// updated only with complete index data.
func (fsi *Index) SaveFile(path string) error {
	dir, name := filepath.Dir(path), filepath.Base(path)
	f, err := ioutil.TempFile(dir, name + ".tmp")
	if err != nil {
		return &IndexSaveError{err}	// XXX needed?
	}

	// TODO use buffering for f (ogórek does not buffer itself on encoding)
	err1 := fsi.Save(f)
	err2 := f.Close()
	if err1 != nil || err2 != nil {
		os.Remove(f.Name())
		err = err1
		if err == nil {
			err = &IndexSaveError{err2}	// XXX needed?
		}
		return err
	}

	err = os.Rename(f.Name(), path)
	if err != nil {
		return &IndexSaveError{err}	// XXX needed?
	}

	return nil
}

// IndexLoadError is the error type returned by index load routines
type IndexLoadError struct {
	Filename string
	Pos      int64
	Err	 error
}

func (e *IndexLoadError) Error() string {
	s := "index load: "
	if e.Filename != "" {
		s += e.Filename + ": "
	}
	if e.Pos != -1 {
		s += "pickle @" + strconv.FormatInt(e.Pos, 10) + ": "
	}
	s += e.Err.Error()
	return s
}

// xint64 tries to convert unpickled value to int64
func xint64(xv interface{}) (v int64, ok bool) {
	switch v := xv.(type) {
	case int64:
		return v, true
	case *big.Int:
		if v.IsInt64() {
			return v.Int64(), true
		}
	}

	return 0, false
}

// LoadIndex loads index from a reader
func LoadIndex(r io.Reader) (fsi *Index, err error) {
	var picklePos int64

	{
		var ok bool
		var xtopPos, xv interface{}

		xr := xbufio.NewReader(r)
		// by passing bufio.Reader directly we make sure it won't create one internally
		p := pickle.NewDecoder(xr.Reader)

		picklePos = xr.InputOffset()
		xtopPos, err = p.Decode()
		if err != nil {
			goto out
		}
		topPos, ok := xint64(xtopPos)
		if !ok {
			err = fmt.Errorf("topPos is %T:%v  (expected int64)", xtopPos, xtopPos)
			goto out
		}

		fsi = IndexNew()
		fsi.TopPos = topPos
		var oidb [8]byte

	loop:
		for {
			// load/decode next entry
			var v pickle.Tuple
			picklePos = xr.InputOffset()
			xv, err = p.Decode()
			if err != nil {
				goto out
			}

			switch xv := xv.(type) {
			default:
				err = fmt.Errorf("invalid entry: type %T", xv)
				goto out

			case pickle.None:
				break loop

			// we accept tuple or list
			// XXX accept only tuple ?
			case pickle.Tuple:
				v = xv
			case []interface{}:
				v = pickle.Tuple(xv)
			}

			// unpack entry tuple -> oidPrefix, fsBucket
			if len(v) != 2 {
				err = fmt.Errorf("invalid entry: len = %i", len(v))
				goto out
			}

			// decode oidPrefix
			xoidPrefixStr := v[0]
			oidPrefixStr, ok := xoidPrefixStr.(string)
			if !ok {
				err = fmt.Errorf("invalid oidPrefix: type %T", xoidPrefixStr)
				goto out
			}
			if l := len(oidPrefixStr); l != 6 {
				err = fmt.Errorf("invalid oidPrefix: len = %i", l)
				goto out
			}
			copy(oidb[:], oidPrefixStr)
			oidPrefix := zodb.Oid(binary.BigEndian.Uint64(oidb[:]))

			// check fsBucket
			xkvStr := v[1]
			kvStr, ok := xkvStr.(string)
			if !ok {
				err = fmt.Errorf("invalid fsBucket: type %T", xkvStr)
				goto out
			}
			if l := len(kvStr); l % 8 != 0 {
				err = fmt.Errorf("invalid fsBucket: len = %i", l)
				goto out
			}

			// load btree from fsBucket entries
			kvBuf := mem.Bytes(kvStr)

			n := len(kvBuf) / 8
			oidBuf := kvBuf[:n*2]
			posBuf := kvBuf[n*2-2:] // NOTE starting 2 bytes behind

			for i:=0; i<n; i++ {
				oid := zodb.Oid(binary.BigEndian.Uint16(oidBuf[i*2:]))
				oid |= oidPrefix
				pos := int64(binary.BigEndian.Uint64(posBuf[i*6:]) & posValidMask)

				fsi.Set(oid, pos)
			}
		}
	}

out:
	if err == nil {
		return fsi, err
	}

	return nil, &IndexLoadError{xio.Name(r), picklePos, err}
}

// LoadIndexFile loads index from a file @ path
func LoadIndexFile(path string) (fsi *Index, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, &IndexLoadError{path, -1, err}
	}

	defer func() {
		err2 := f.Close()
		if err2 != nil && err == nil {
			err = &IndexLoadError{path, -1, err}
			fsi = nil
		}
	}()

	// NOTE no explicit bufferring needed - ogórek and LoadIndex use bufio.Reader internally
	return LoadIndex(f)
}

// ----------------------------------------

// Equal returns whether two indices are the same
func (a *Index) Equal(b *Index) bool {
	if a.TopPos != b.TopPos {
		return false
	}

	return treeEqual(a.Tree, b.Tree)
}

// treeEqual returns whether two fsb.Tree are the same
func treeEqual(a, b *fsb.Tree) bool {
	if a.Len() != b.Len() {
		return false
	}

	ea, _ := a.SeekFirst()
	eb, _ := b.SeekFirst()

	if ea == nil {
		// this means len(a) == 0 -> len(b) == 0 -> eb = nil
		return true
	}

	defer ea.Close()
	defer eb.Close()

	for {
		ka, va, stopa := ea.Next()
		kb, vb, stopb := eb.Next()

		if stopa != nil || stopb != nil {
			if stopa != stopb {
				panic("same-length trees iteration did not end at the same time")
			}
			break
		}

		if !(ka == kb && va == vb) {
			return false
		}
	}

	return true
}

// --- build index from FileStorage data ---

// Update updates in-memory index from r's FileStorage data in byte-range index.TopPos..topPos
//
// The use case is: we have index computed till some position; we open
// FileStorage and see there is more data; we update index from data range
// not-yet covered by the index.
//
// topPos=-1 means data range to update from is index.TopPos..EOF
//
// The index stays valid even in case of error - then index is updated but only
// partially. The index always stays consistent as updates to it are applied as
// a whole for every data transaction. On return index.TopPos indicates till
// which position in data the index could be updated.
//
// On success returned error is nil and index.TopPos is set to either:
// - topPos (if it is != -1), or
// - r's position at which read got EOF (if topPos=-1).
func (index *Index) Update(ctx context.Context, r io.ReaderAt, topPos int64) (err error) {
	defer xerr.Contextf(&err, "%s: reindex %v..%v", xio.Name(r), index.TopPos, topPos)

	if topPos >= 0 && index.TopPos > topPos {
	     return fmt.Errorf("backward update requested")
	}

	// XXX another way to compute index: iterate backwards - then
	// 1. index entry for oid is ready right after we see oid the first time
	// 2. we can be sure we build the whole index if we saw all oids

	it := Iterate(r, index.TopPos, IterForward)
	for {
		// check ctx cancel once per transaction
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// iter to next txn
		err = it.NextTxn(LoadNoStrings)
		if err != nil {
			err = okEOF(err)
			if err == nil {
				// if EOF earlier topPos -> error
				if topPos >= 0 && index.TopPos < topPos {
					err = fmt.Errorf("unexpected EOF @%v", index.TopPos)
				}
			}
			return err
		}

		// XXX check txnh.Status != TxnInprogress

		// check for topPos overlapping txn & whether we are done.
		// topPos=-1 will never match here
		if it.Txnh.Pos < topPos && (it.Txnh.Pos + it.Txnh.Len) > topPos {
			return fmt.Errorf("transaction %v @%v overlaps topPos boundary",
				it.Txnh.Tid, it.Txnh.Pos)
		}
		if it.Txnh.Pos == topPos {
			return nil
		}

		// collect data for index update in temporary place.
		// do not update the index immediately so that in case of error
		// in the middle of txn's data, index stays consistent and
		// correct for topPos pointing to previous transaction.
		update := map[zodb.Oid]int64{}	// XXX malloc every time -> better reuse
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
		}

		// update index "atomically" with data from just read transaction
		index.TopPos = it.Txnh.Pos + it.Txnh.Len
		for oid, pos := range update {
			index.Set(oid, pos)
		}
	}

	return nil
}

// BuildIndex builds new in-memory index for data in r
//
// non-nil valid and consistent index is always returned - even in case of error
// the index will describe data till top-position of highest transaction that
// could be read without error.
//
// In such cases the index building could be retried to be finished with
// index.Update().
func BuildIndex(ctx context.Context, r io.ReaderAt) (*Index, error) {
	index := IndexNew()
	err := index.Update(ctx, r, -1)
	return index, err
}

// BuildIndexForFile builds new in-memory index for data in file @ path
//
// See BuildIndex for semantic description.
func BuildIndexForFile(ctx context.Context, path string) (index *Index, err error) {
	f, err := os.Open(path)
	if err != nil {
		return IndexNew(), err // XXX add err ctx?
	}

	defer func() {
		err2 := f.Close()
		err = xerr.First(err, err2)
	}()

	// use IO optimized for sequential access when building index
	fSeq := xbufio.NewSeqReaderAt(f)

	return BuildIndex(ctx, fSeq)
}

// --- verify index against data in FileStorage ---

// IndexCorrupyError is the error type returned by index verification routines
// when index was found to not match original FileStorage data.
type IndexCorruptError struct {
	DataFileName string
	Detail       string
}

func (e *IndexCorruptError) Error() string {
	return fmt.Sprintf("%s: verify index: %s", e.DataFileName, e.Detail)
}

func indexCorrupt(r io.ReaderAt, format string, argv ...interface{}) *IndexCorruptError {
	return &IndexCorruptError{DataFileName: xio.Name(r), Detail: fmt.Sprintf(format, argv...)}
}

// VerifyTail checks index correctness against several newest transactions of FileStorage data in r.
//
// For (XXX max) ntxn transactions starting from index.TopPos backwards, it verifies
// whether oid there have correct entries in the index.
//
// ntxn=-1 means data range to verify is till start of the file.
//
// Returned error is either:
// - of type *IndexCorruptError, when data in index was found not to match original data, or
// - any other error type representing e.g. IO error when reading original data or something else.
func (index *Index) VerifyTail(ctx context.Context, r io.ReaderAt, ntxn int) (oidChecked map[zodb.Oid]struct{}, err error) {
	defer func() {
		if _, ok := err.(*IndexCorruptError); ok {
			return // leave it as is
		}

		xerr.Contextf(&err, "%s: verify index @%v~{%v}", xio.Name(r), index.TopPos, ntxn)
	}()

	oidChecked = map[zodb.Oid]struct{}{} // Set<zodb.Oid>

	it := Iterate(r, index.TopPos, IterBackward)
	for i := 0; ntxn == -1 || i < ntxn; i++ {
		// check ctx cancel once per transaction
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		err := it.NextTxn(LoadNoStrings)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err		// XXX err ctx
		}

		for {
			err = it.NextData()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err	// XXX err ctx
			}

			// if oid was already checked - do not check index anymore
			// (index has info only about latest entries)
			if _, ok := oidChecked[it.Datah.Oid]; ok {
				continue
			}
			oidChecked[it.Datah.Oid] = struct{}{}

			dataPos, ok := index.Get(it.Datah.Oid)
			if !ok {
				return nil, indexCorrupt(r, "oid %v @%v: no index entry",
					it.Datah.Oid, it.Datah.Pos)
			}

			if dataPos != it.Datah.Pos {
				return nil, indexCorrupt(r, "oid %v @%v: index has wrong pos (%v)",
					it.Datah.Oid, it.Datah.Pos, dataPos)
			}
		}
	}

	// TODO err = EOF -> merge from Verify

	return oidChecked, nil
}

// Verify checks index correctness against FileStorage data in r.
//
// it verifies whether index is exactly the same as if it was build anew for
// data in range ..index.TopPos .
//
// See VerifyTail for description about errors returned.
func (index *Index) Verify(ctx context.Context, r io.ReaderAt) error {
	oidChecked, err := index.VerifyTail(ctx, r, -1)
	if err != nil {
		return err
	}

	// XXX merge this into VerifyTail

	// all oids from data were checked to be in index
	// now verify that there is no extra oids in index
	if len(oidChecked) == index.Len() {
		return nil
	}

	e, _ := index.SeekFirst() // !nil as nil means index.Len=0 and len(oidChecked) <= index.Len
	defer e.Close()

	for {
		oid, pos, errStop := e.Next()
		if errStop != nil {
			break
		}

		if _, ok := oidChecked[oid]; !ok {
			return indexCorrupt(r, "oid %v @%v: present in index but not in data", oid, pos)
		}
	}

	return nil
}

// XXX text
func (index *Index) VerifyTailForFile(ctx context.Context, path string, ntxn int) (oidChecked map[zodb.Oid]struct{}, err error) {
	err = index.verifyForFile(path, func(r io.ReaderAt) error {
		oidChecked, err = index.VerifyTail(ctx, r, ntxn)
		return err
	})
	return
}

// VerifyForFile verifies index correctness against FileStorage file @ path
// XXX text
func (index *Index) VerifyForFile(ctx context.Context, path string) error {
	return index.verifyForFile(path, func(r io.ReaderAt) error {
		return index.Verify(ctx, r)
	})
}

// common driver for Verify*ForFile
func (index *Index) verifyForFile(path string, check func(r io.ReaderAt) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	defer func() {
		err2 := f.Close()
		err = xerr.First(err, err2)
	}()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	topPos := fi.Size()	// XXX there might be last TxnInprogress transaction
	if index.TopPos != topPos {
		return indexCorrupt(f, "topPos mismatch: data=%v  index=%v", topPos, index.TopPos)
	}

	// use IO optimized for sequential access when verifying index
	fSeq := xbufio.NewSeqReaderAt(f)

	return check(fSeq)
}
