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

package fs1
// index for quickly finding oid -> oid's latest data record

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1/fsb"

	pickle "github.com/kisielk/og-rek"
	"lab.nexedi.com/kirr/neo/go/zodb/internal/pickletools"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/go123/xbufio"
	"lab.nexedi.com/kirr/go123/xerr"
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

// IndexNew creates new empty index.
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
	oidPrefixMask  zodb.Oid = (1<<64 - 1) ^ (1<<16 - 1) // 0xffffffffffff0000
	posInvalidMask uint64   = (1<<64 - 1) ^ (1<<48 - 1) // 0xffff000000000000
	posValidMask   uint64   = 1<<48 - 1                 // 0x0000ffffffffffff
)

// IndexSaveError is the error type returned by index save routines.
type IndexSaveError struct {
	Err error // error that occurred during the operation
}

func (e *IndexSaveError) Error() string {
	return "index save: " + e.Err.Error()
}

// Save saves index to a writer.
func (fsi *Index) Save(w io.Writer) (err error) {
	defer func() {
		if err == nil {
			return
		}

		if _, ok := err.(*pickle.TypeError); ok {
			panic(err) // all our types are expected to be supported by pickle
		}

		// otherwise it is an error returned by writer, which should already
		// have filename & op as context.
		err = &IndexSaveError{err}
	}()

	p := pickle.NewEncoder(w)

	err = p.Encode(fsi.TopPos)
	if err != nil {
		return err
	}

	var oidb [8]byte
	var posb [8]byte
	var oidPrefixCur zodb.Oid // current oid[0:6] with [6:8] = 00
	oidBuf := []byte{}        // current oid[6:8]oid[6:8]...
	posBuf := []byte{}        // current pos[2:8]pos[2:8]...
	var t [2]interface{}      // tuple for (oid, fsBucket.toString())

	e, _ := fsi.SeekFirst()
	if e != nil {
		defer e.Close()

		for {
			oid, pos, errStop := e.Next()
			oidPrefix := oid & oidPrefixMask

			if oidPrefix != oidPrefixCur || errStop != nil {
				// emit (oid[0:6], oid[6:8]oid[6:8]...pos[2:8]pos[2:8]...)
				binary.BigEndian.PutUint64(oidb[:], uint64(oidPrefixCur))
				t[0] = string(oidb[0:6])
				t[1] = string(bytes.Join([][]byte{oidBuf, posBuf}, nil))
				err = p.Encode(pickle.Tuple(t[:]))
				if err != nil {
					return err
				}

				oidPrefixCur = oidPrefix
				oidBuf = oidBuf[:0]
				posBuf = posBuf[:0]
			}

			if errStop != nil {
				break
			}

			// check pos does not overflow 6 bytes
			if uint64(pos)&posInvalidMask != 0 {
				return fmt.Errorf("entry position too large: 0x%x", pos)
			}

			binary.BigEndian.PutUint64(oidb[:], uint64(oid))
			binary.BigEndian.PutUint64(posb[:], uint64(pos))

			oidBuf = append(oidBuf, oidb[6:8]...)
			posBuf = append(posBuf, posb[2:8]...)
		}
	}

	err = p.Encode(pickle.None{})
	return err
}

// SaveFile saves index to a file @ path.
//
// Index data is first saved to a temporary file and when complete the
// temporary is renamed to be at requested path. This way file @ path will be
// updated only with complete index data.
func (fsi *Index) SaveFile(path string) error {
	dir, name := filepath.Dir(path), filepath.Base(path)
	f, err := ioutil.TempFile(dir, name+".tmp")
	if err != nil {
		return &IndexSaveError{err}
	}

	// use buffering for f (ogórek does not buffer itself on encoding)
	fb := bufio.NewWriter(f)

	err1 := fsi.Save(fb)
	err2 := fb.Flush()
	err3 := f.Close()
	if err1 != nil || err2 != nil || err3 != nil {
		os.Remove(f.Name())
		err = err1
		if err == nil {
			err = &IndexSaveError{xerr.First(err2, err3)}
		}
		return err
	}

	err = os.Rename(f.Name(), path)
	if err != nil {
		return &IndexSaveError{err}
	}

	return nil
}

// IndexLoadError is the error type returned by index load routines.
type IndexLoadError struct {
	Filename string // present if used IO object was with .Name()
	Pos      int64
	Err      error
}

func (e *IndexLoadError) Error() string {
	s := "index load: "
	if e.Filename != "" && e.Pos != -1 /* not yet got to decoding - .Err is ~ os.PathError */ {
		s += e.Filename + ": "
	}
	if e.Pos != -1 {
		s += "pickle @" + strconv.FormatInt(e.Pos, 10) + ": "
	}
	s += e.Err.Error()
	return s
}

// LoadIndex loads index from a reader.
func LoadIndex(r io.Reader) (fsi *Index, err error) {
	var picklePos int64
	defer func() {
		if err != nil {
			err = &IndexLoadError{ioname(r), picklePos, err}
		}
	}()

	var ok bool
	var xtopPos, xv interface{}

	xr := xbufio.NewReader(r)
	// by passing bufio.Reader directly we make sure it won't create one internally
	p := pickle.NewDecoder(xr.Reader)

	picklePos = xr.InputOffset()
	xtopPos, err = p.Decode()
	if err != nil {
		return nil, err
	}
	topPos, ok := pickletools.Xint64(xtopPos)
	if !ok {
		return nil, fmt.Errorf("topPos is %T:%v  (expected int64)", xtopPos, xtopPos)
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
			return nil, err
		}

		switch xv := xv.(type) {
		default:
			return nil, fmt.Errorf("invalid entry: type %T", xv)

		case pickle.None:
			break loop

		// we accept tuple or list
		case pickle.Tuple:
			v = xv
		case []interface{}:
			v = pickle.Tuple(xv)
		}

		// unpack entry tuple -> oidPrefix, fsBucket
		if len(v) != 2 {
			return nil, fmt.Errorf("invalid entry: len = %d", len(v))
		}

		// decode oidPrefix
		xoidPrefixStr := v[0]
		oidPrefixStr, ok := xoidPrefixStr.(string)
		if !ok {
			return nil, fmt.Errorf("invalid oidPrefix: type %T", xoidPrefixStr)
		}
		if l := len(oidPrefixStr); l != 6 {
			return nil, fmt.Errorf("invalid oidPrefix: len = %d", l)
		}
		copy(oidb[:], oidPrefixStr)
		oidPrefix := zodb.Oid(binary.BigEndian.Uint64(oidb[:]))

		// check fsBucket
		xkvStr := v[1]
		kvStr, ok := xkvStr.(string)
		if !ok {
			return nil, fmt.Errorf("invalid fsBucket: type %T", xkvStr)
		}
		if l := len(kvStr); l%8 != 0 {
			return nil, fmt.Errorf("invalid fsBucket: len = %d", l)
		}

		// load btree from fsBucket entries
		kvBuf := mem.Bytes(kvStr)

		n := len(kvBuf) / 8
		oidBuf := kvBuf[:n*2]
		posBuf := kvBuf[n*2-2:] // NOTE starting 2 bytes behind

		for i := 0; i < n; i++ {
			oid := zodb.Oid(binary.BigEndian.Uint16(oidBuf[i*2:]))
			oid |= oidPrefix
			pos := int64(binary.BigEndian.Uint64(posBuf[i*6:]) & posValidMask)

			fsi.Set(oid, pos)
		}
	}

	return fsi, nil
}

// LoadIndexFile loads index from a file @ path.
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

// Equal returns whether two indices are the same.
func (a *Index) Equal(b *Index) bool {
	if a.TopPos != b.TopPos {
		return false
	}

	return treeEqual(a.Tree, b.Tree)
}

// treeEqual returns whether two fsb.Tree are the same.
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

// IndexUpdateProgress is data sent by Index.Update to notify about progress.
type IndexUpdateProgress struct {
	TopPos     int64  // data range to update to; if = -1 -- till EOF
	TxnIndexed int    // # transactions read/indexed so far
	Index      *Index // index built so far
	Iter       *Iter  // iterator through data	XXX needed?
}

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
// a whole once per data transaction. On return index.TopPos indicates till
// which position in data the index could be updated.
//
// On success returned error is nil and index.TopPos is set to either:
//
//	- topPos (if it is != -1), or
//	- r's position at which read got EOF (if topPos=-1).
func (index *Index) Update(ctx context.Context, r io.ReaderAt, topPos int64, progress func(*IndexUpdateProgress)) (err error) {
	defer xerr.Contextf(&err, "%sreindex %v..%v", ioprefix(r), index.TopPos, topPos)

	if topPos >= 0 && index.TopPos > topPos {
		return fmt.Errorf("backward update requested")
	}

	// XXX another way to compute index: iterate backwards - then
	// 1. index entry for oid is ready right after we see oid the first time
	// 2. we can be sure we build the whole index if we saw all oids

	it := Iterate(r, index.TopPos, IterForward)

	pd := &IndexUpdateProgress{
		TopPos: topPos,
		Index:  index,
		Iter:   it,
	}

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
		if it.Txnh.Pos < topPos && (it.Txnh.Pos+it.Txnh.Len) > topPos {
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
		update := map[zodb.Oid]int64{} // XXX malloc every time -> better reuse
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

		// notify progress
		if progress != nil {
			pd.TxnIndexed++
			progress(pd)
		}
	}

	return nil
}

// BuildIndex builds new in-memory index for data in r.
//
// non-nil valid and consistent index is always returned - even in case of error
// the index will describe data till top-position of highest transaction that
// could be read without error.
//
// In such cases the index building could be retried to be finished with
// index.Update().
func BuildIndex(ctx context.Context, r io.ReaderAt, progress func(*IndexUpdateProgress)) (*Index, error) {
	index := IndexNew()
	err := index.Update(ctx, r, -1, progress)
	return index, err
}

// BuildIndexForFile builds new in-memory index for data in file @ path.
//
// See BuildIndex for semantic description.
func BuildIndexForFile(ctx context.Context, path string, progress func(*IndexUpdateProgress)) (index *Index, err error) {
	f, err := os.Open(path)
	if err != nil {
		return IndexNew(), err
	}

	defer func() {
		err2 := f.Close()
		err = xerr.First(err, err2)
	}()

	// use IO optimized for sequential access when building index
	fSeq := seqReadAt(f)

	return BuildIndex(ctx, fSeq, progress)
}

// --- verify index against data in FileStorage ---

// IndexCorruptError is the error type returned by index verification routines
// when index was found to not match original FileStorage data.
type IndexCorruptError struct {
	DataFileName string // present if data IO object was with .Name()
	Detail       string
}

func (e *IndexCorruptError) Error() string {
	prefix := e.DataFileName
	if prefix != "" {
		prefix += ": "
	}
	return fmt.Sprintf("%sverify index: %s", prefix, e.Detail)
}

func indexCorrupt(f interface{}, format string, argv ...interface{}) *IndexCorruptError {
	return &IndexCorruptError{DataFileName: ioname(f), Detail: fmt.Sprintf(format, argv...)}
}

// IndexVerifyProgress is data sent by Index.Verify to notify about progress.
type IndexVerifyProgress struct {
	TxnTotal   int                   // total # of transactions to verify; if = -1 -- whole data
	TxnChecked int
	Index      *Index                // index verification runs for
	Iter       *Iter                 // iterator through data
	OidChecked map[zodb.Oid]struct{} // oid checked so far
}

// Verify checks index correctness against FileStorage data in r.
//
// For ntxn transactions starting from index.TopPos backwards, it verifies
// whether oid there have correct entries in the index.
//
// ntxn=-1 means data range to verify is till start of the file.
//
// If whole data file was covered (either ntxn is big enough or was set = -1)
// additional checks are performed to make sure there is no extra entries in
// the index. For whole-data file cases Verify thus checks whether index is
// exactly the same as if it was build anew for data in range ..index.TopPos .
//
// Returned error is either:
//
//	- of type *IndexCorruptError, when data in index was found not to match original data, or
//	- any other error type representing e.g. IO error when reading original data or something else.
func (index *Index) Verify(ctx context.Context, r io.ReaderAt, ntxn int, progress func(*IndexVerifyProgress)) (oidChecked map[zodb.Oid]struct{}, err error) {
	defer func() {
		if _, ok := err.(*IndexCorruptError); ok {
			return // leave it as is
		}

		xerr.Contextf(&err, "%sverify index @%v~{%v}", ioprefix(r), index.TopPos, ntxn)
	}()

	oidChecked = map[zodb.Oid]struct{}{} // Set<zodb.Oid>
	wholeData := false

	it := Iterate(r, index.TopPos, IterBackward)

	pd := &IndexVerifyProgress{
		TxnTotal:   ntxn,
		Index:      index,
		Iter:       it,
		OidChecked: oidChecked,
	}

	for i := 0; ntxn == -1 || i < ntxn; i++ {
		// check ctx cancel once per transaction
		select {
		case <-ctx.Done():
			return oidChecked, ctx.Err()
		default:
		}

		err := it.NextTxn(LoadNoStrings)
		if err != nil {
			if err == io.EOF {
				wholeData = true
				break
			}
			return oidChecked, err
		}

		for {
			err = it.NextData()
			if err != nil {
				if err == io.EOF {
					break
				}
				return oidChecked, err
			}

			// if oid was already checked - do not check index anymore
			// (index has info only about latest entries)
			if _, ok := oidChecked[it.Datah.Oid]; ok {
				continue
			}
			oidChecked[it.Datah.Oid] = struct{}{}

			dataPos, ok := index.Get(it.Datah.Oid)
			if !ok {
				return oidChecked, indexCorrupt(r, "oid %v @%v: no index entry",
					it.Datah.Oid, it.Datah.Pos)
			}

			if dataPos != it.Datah.Pos {
				return oidChecked, indexCorrupt(r, "oid %v @%v: index has wrong pos (%v)",
					it.Datah.Oid, it.Datah.Pos, dataPos)
			}
		}

		// notify progress
		if progress != nil {
			pd.TxnChecked++
			progress(pd)
		}
	}

	// all oids from data were checked to be in index
	// now verify that there is no extra oids in index
	if wholeData && len(oidChecked) != index.Len() {
		// !nil as nil means index.Len=0 and len(oidChecked) < index.Len here
		e, _ := index.SeekFirst()
		defer e.Close()

		for {
			oid, pos, errStop := e.Next()
			if errStop != nil {
				break
			}

			if _, ok := oidChecked[oid]; !ok {
				return oidChecked, indexCorrupt(r, "oid %v @%v: present in index but not in data", oid, pos)
			}
		}
	}

	return oidChecked, nil
}

// VerifyForFile checks index correctness against FileStorage data in file @ path.
//
// See Verify for semantic description.
func (index *Index) VerifyForFile(ctx context.Context, path string, ntxn int, progress func(*IndexVerifyProgress)) (oidChecked map[zodb.Oid]struct{}, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer func() {
		err2 := f.Close()
		err = xerr.First(err, err2)
	}()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	topPos := fi.Size() // XXX there might be last TxnInprogress transaction	XXX
	if index.TopPos != topPos {
		return nil, indexCorrupt(f, "topPos mismatch: data=%v  index=%v", topPos, index.TopPos)
	}

	// use IO optimized for sequential access when verifying index
	fSeq := seqReadAt(f)

	return index.Verify(ctx, fSeq, ntxn, progress)
}
