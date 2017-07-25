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

package fs1tools

import (
	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"

	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xfmt"
)

/*
Dump dumps transactions from a FileStorage.

Format is the same as in fsdump/py originally written by Jeremy Hylton:

	https://github.com/zopefoundation/ZODB/blob/master/src/ZODB/FileStorage/fsdump.py
	https://github.com/zopefoundation/ZODB/commit/ddcb46a2
	https://github.com/zopefoundation/ZODB/commit/4d86e4e0
*/
func Dump(w io.Writer, path string, options DumpOptions) (err error) {
	var d dumper
	if options.Verbose {
		d = &dumperVerbose{}
	} else {
		d = &dumper1{}
	}

	return dump(w, path, d)
}

type DumpOptions struct {
	Verbose bool // dump in verbose mode
}

func dump(w io.Writer, path string, d dumper) (err error) {
	defer xerr.Contextf(&err, "%s: fsdump", path)	// XXX ok?

	fs, err := fs1.Open(path, read-only, no-index)
	if err != nil {
		return err
	}
	defer func() {
		err2 := fs.Close()
		err = xerr.First(err, err2)
	}()

	// buffer for formatting
	buf := &xfmt.Buffer{}
	flushBuf := func() error {
		err := w.Write(buf.Bytes())
		buf.Reset()
		return err
	}

	// make sure to flush buffer if we return prematurely e.g. with an error
	defer func() {
		err2 := flushBuf()
		err = xerr.First(err, err2)
	}()

	// TODO d.dumpFileHeader


	it := fs.IterateRaw(fwd)
	for i := 0; ; i++ {
		err = it.NextTxn(fs1.LoadAll)
		if err != nil {
			if err == io.EOF {
				err = nil	// XXX -> okEOF(err)
			}
			return err
		}

		d.dumpTxn(buf, it.Txnh)	// XXX err

		for j := 0; ; j++ {
			err = it.NextData()
			if err != nil {
				if err == io.EOF {
					err = nil	// XXX -> okEOF(err)
					break
				}
				return err
			}

			d.dumpData(buf, it.Datah)	// XXX err

		}

		d.dumpTxnPost(buf, it.Txnh)	// XXX err

		err = flushBuf()
		if err != nil {
			return err
		}
	}
}

// dumper is internal interface to implement various dumping modes
type dumper interface {
	dumpFileHeader(buf *xfmt.Buffer, *fs1.FileHeader) error
	dumpTxn(buf *xfmt.Buffer, *fs1.TxnHeader) error
	dumpData(buf *xfmt.Buffer, *fs1.DataHeader) error
	dumpTxnPost(buf *xfmt.Buffer, *fs1.TxnHeader) error
}

// "normal" dumper
type dumper1 struct {
}

func (d *dumper1) dumpFileHeader(buf *xfmt.Buffer, fh *fs1.FileHeader) error {
	return nil
}

func (d *dumper1) dumpTxn(buf *xfmt.Buffer, txnh *fs1.TxnHeader) error {
	buf .S("Trans #") .D_f("05", i) .S(" tid=") .V(it.Txnh.Tid)
	buf .S(" time=") .V(it.Txnh.Tid.Time()) .S(" offset=") .D64(it.Txnh.Pos)
	buf .S("\n    status=") .Qpy(it.Txnh.Status)
	buf .S(" user=") .Qpyb(it.Txnh.User)
	buf .S(" description=") .Qpyb(it.Txnh.Description) .S("\n")
}

func (d *dumper1) dumpData(buf *xfmt.Buffer, dh *fs1.DataHeader) error {
	buf .S("  data #") .D_f("05", j) .S(" oid=") .V(it.Datah.Oid)

	if it.Datah.DataLen == 0 {
		buf .S(" class=undo or abort of object creation")

		backPos, err := it.Datah.LoadBackRef()
		if err != nil {
			// XXX
		}

		if backPos != 0 {
			buf .S(" bp=") .X016(uint64(backPos))
		}
	} else {
		// XXX Datah.LoadData()
		modname, classname = zodb.GetPickleMetadata(...)	// XXX
		fullclass = "%s.%s" % (modname, classname)

		buf .S(" size=") .D(len(...))
		buf .S(" class=") .S(fullclass)
	}

	buf .S("\n")
}

func (d *dumper1) dumpTxnPost(buf *xfmt.Buffer, txnh *fs1.TxnHeader) error {
	return nil
}

// ----------------------------------------

type dumperVerbose struct {
}

func (d *dumperVerbose) dumpFileHeader(buf *xfmt.Buffer, fh *fs1.FileHeader) error {
	buf .S("*" * 60) .S("\n")
	buf .S("file identifier: ") .Qpyb(fh.Magic) .S("\n")
	return nil
}

func (d *dumperVerbose) dumpTxn(buf *xfmt.Buffer, txnh *fs1.TxnHeader) error {
	buf .S("=" * 60)
	buf .S("\noffset: ") .D64(it.Txnh.Pos)
	buf .S("\nend pos: ") .D64(it.Txnh.Pos + it.Txnh.Len)
	buf .S("\ntransaction id: ") .V(it.Txnh.Tid)
	buf .S("\ntrec len: ") .D64(it.Txnh.Len)
	buf .S("\nstatus: ") .Qpy(it.Txnh.Status)
	buf .S("\nuser: ") .Qpyb(it.Txnh.User)
	buf .S("\ndescription: ") .Qpyb(it.Txnh.Description)
	buf .S("\nlen(extra): ") .D(len(it.Txnh.Extension))
	buf .S("\n")
	return nil
}

func (d *dumperVerbose) dumpData(buf *xfmt.Buffer, dh *fs1.DataHeader) error {
	buf .S("-" * 60)
	buf .S("\noffset: ") .D64(dh.Pos)
	buf .S("\noid: ") .V(dh.Oid)
	buf .S("\nrevid: "). V(dh.Tid)
	buf .S("\nprevious record offset: ") .D64(dh.PrevRevPos)
	buf .S("\ntransaction offset: ") .D64(dh.TxnPos)
	buf .S("\nlen(data): ") .D64(dh.DataLen)

	if dh.DataLen == 0 {
		backPos, err := dh.LoadBackRef()
		if err != nil {
			// XXX
		}

		buf .S("\nbackpointer: ", D64(backPos))
	}

	buf .S("\n")
	return nil
}

func (d *dumperVerbose) dumpTxnPost(buf *xfmt.Buffer, txnh *fs1.TxnHeader) error {
	// NOTE printing the same .Len twice
	// we do not print/check redundant len here because our
	// FileStorage code checks/reports this itself
	buf .S("redundant trec len: " .D64(it.Txnh.Len)) .S("\n")
	return nil
}
