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
// various dumping routines / subcommands

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"os"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"

	"lab.nexedi.com/kirr/go123/prog"
	"lab.nexedi.com/kirr/go123/xbytes"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xfmt"
)

// Dumper is interface to implement various dumping modes
type Dumper interface {
	// DumperName defines concise name to use in error-reporting when using this dumper.
	DumperName() string

	// DumpFileHeader dumps fh to buf
	DumpFileHeader(buf *xfmt.Buffer, fh *fs1.FileHeader) error

	// DumpTxn dumps current transaction from it to buf.
	//
	// It is dumper responsibility to iterate over data records inside
	// transaction if it needs to dump information about data records.
	//
	// If dumper return io.EOF the whole dumping process finishes.
	DumpTxn(buf *xfmt.Buffer, it *fs1.Iter) error
}

// Dump dumps content of a FileStorage file @ path.
//
// To do so it reads file header and then iterates over all transactions in the file.
// The logic to actually output information and, if needed read/process data, is implemented by Dumper d.
func Dump(w io.Writer, path string, dir fs1.IterDir, d Dumper) (err error) {
	defer xerr.Contextf(&err, "%s: %s", d.DumperName(), path)

	it, f, err := fs1.IterateFile(path, dir)
	if err != nil {
		return err
	}

	defer func() {
		err2 := f.Close()
		err = xerr.First(err, err2)
	}()

	// buffer for formatting
	buf := &xfmt.Buffer{}
	flushBuf := func() error {
		_, err := w.Write(buf.Bytes())
		buf.Reset()
		return err
	}

	// make sure to flush buffer on return
	defer func() {
		err2 := flushBuf()
		err = xerr.First(err, err2)
	}()


	// file header
	var fh fs1.FileHeader
	err = fh.Load(it.R)
	if err != nil {
		return err
	}
	err = d.DumpFileHeader(buf, &fh)
	if err != nil {
		return err
	}

	// iter over txn/data
	for {
		err = it.NextTxn(fs1.LoadAll)
		if err != nil {
			if err == io.EOF {
				err = nil	// XXX -> okEOF(err)
			}
			return err
		}

		err = d.DumpTxn(buf, it)
		if err != nil {
			if err == io.EOF {
				err = nil	// XXX -> okEOF(err)
			}
			return err
		}

		err = flushBuf()
		if err != nil {
			return err
		}
	}
}

// ----------------------------------------

// DumperFsDump implements dumping with the same format as in fsdump/py
// originally written by Jeremy Hylton:
//
//	https://github.com/zopefoundation/ZODB/blob/master/src/ZODB/FileStorage/fsdump.py
//	https://github.com/zopefoundation/ZODB/commit/ddcb46a2
type DumperFsDump struct {
	ntxn  int // current transaction record #

	// for loading data
	dhLoading fs1.DataHeader
}

func (d *DumperFsDump) DumperName() string {
	return "fsdump"
}

func (d *DumperFsDump) DumpFileHeader(buf *xfmt.Buffer, fh *fs1.FileHeader) error {
	return nil
}

func (d *DumperFsDump) DumpTxn(buf *xfmt.Buffer, it *fs1.Iter) error {
	txnh := &it.Txnh
	buf .S("Trans #")
	buf .S(fmt.Sprintf("%05d", d.ntxn))	// XXX -> .D_f("05", d.ntxn)
	buf .S(" tid=") .V(txnh.Tid)
	buf .S(" time=") .V(txnh.Tid.Time())

	// XXX here fsdump/py prints position of first data record, NOT transaction start!
	buf .S(" offset=") .D64(/*txnh.Pos*/ txnh.DataPos())

	buf .S("\n    status=") .Qpycb(byte(txnh.Status))
	buf .S(" user=") .Qpyb(txnh.User)
	buf .S(" description=") .Qpyb(txnh.Description) .S("\n")
	d.ntxn++

	for j := 0; ; j++ {
		err := it.NextData()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		dh := &it.Datah
		buf .S("  data #")
		buf .S(fmt.Sprintf("%05d", j))	// XXX -> .D_f("05", j)
		buf .S(" oid=") .V(dh.Oid)

		// load actual data
		d.dhLoading = *dh
		dbuf, err := d.dhLoading.LoadData(it.R)
		if err != nil {
			return err
		}

		if dbuf.Data == nil {
			buf .S(" class=undo or abort of object creation")
		} else {
			fullclass := zodb.PyData(dbuf.Data).ClassName()

			buf .S(" size=") .D64(d.dhLoading.DataLen)
			buf .S(" class=") .S(fullclass)
		}

		if dh.DataLen == 0 && dbuf.Data != nil {
			// it was backpointer - print tid of transaction it points to
			buf .S(" bp=") .V(d.dhLoading.Tid)
		}

		buf .S("\n")
		dbuf.Release()
	}

	return nil
}

// DumperFsDumpVerbose implements a very verbose dumper with output identical
// to fsdump.Dumper in zodb/py originally written by Jeremy Hylton:
//
//	https://github.com/zopefoundation/ZODB/blob/master/src/ZODB/FileStorage/fsdump.py
//	https://github.com/zopefoundation/ZODB/commit/4d86e4e0
type DumperFsDumpVerbose struct {
}

func (d *DumperFsDumpVerbose) DumperName() string {
	return "fsdumpv"
}

func (d *DumperFsDumpVerbose) DumpFileHeader(buf *xfmt.Buffer, fh *fs1.FileHeader) error {
	for i := 0; i < 60; i++ {
		buf .S("*")
	}
	buf .S("\n")
	buf .S("file identifier: ") .Qpyb(fh.Magic[:]) .S("\n")
	return nil
}

func (d *DumperFsDumpVerbose) DumpTxn(buf *xfmt.Buffer, it *fs1.Iter) error {
	txnh := &it.Txnh
	for i := 0; i < 60; i++ {
		buf .S("=")
	}
	buf .S("\noffset: ") .D64(txnh.Pos)
	buf .S("\nend pos: ") .D64(txnh.Pos + txnh.Len /* py does not account for redundant len size */- 8)
	buf .S("\ntransaction id: ") .V(txnh.Tid)
	buf .S("\ntrec len: ") .D64(txnh.Len /* py len ^^^ */- 8)
	buf .S("\nstatus: ") .Qpycb(byte(txnh.Status))
	buf .S("\nuser: ") .Qpyb(txnh.User)
	buf .S("\ndescription: ") .Qpyb(txnh.Description)
	buf .S("\nlen(extra): ") .D(len(txnh.Extension))
	buf .S("\n")

	for {
		err := it.NextData()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		err = d.dumpData(buf, it)
		if err != nil {
			return err
		}
	}

	// NOTE printing the same .Len twice
	// we do not print/check redundant len here because our
	// FileStorage code checks/reports this itself
	buf .S("redundant trec len: ") .D64(it.Txnh.Len /* py len ^^^ */- 8) .S("\n")
	return nil
}

func (d *DumperFsDumpVerbose) dumpData(buf *xfmt.Buffer, it *fs1.Iter) error {
	dh := &it.Datah
	for i := 0; i < 60; i++ {
		buf .S("-")
	}
	buf .S("\noffset: ") .D64(dh.Pos)
	buf .S("\noid: ") .V(dh.Oid)
	buf .S("\nrevid: "). V(dh.Tid)
	buf .S("\nprevious record offset: ") .D64(dh.PrevRevPos)
	buf .S("\ntransaction offset: ") .D64(dh.TxnPos)
	buf .S("\nlen(data): ") .D64(dh.DataLen)

	if dh.DataLen == 0 {
		backPos, err := dh.LoadBackRef(it.R)
		if err != nil {
			return err	// XXX err ctx
		}

		buf .S("\nbackpointer: ") .D64(backPos)
	}

	buf .S("\n")
	return nil
}

const dumpSummary = "dump database transactions"

func dumpUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: fs1 dump [options] <storage>
Dump transactions from a FileStorage

<storage> is a path to FileStorage

  options:

	-h --help       this help text.
	-v		verbose mode.
`)
}

func dumpMain(argv []string) {
	var verbose bool
	flags := flag.FlagSet{Usage: func() { dumpUsage(os.Stderr) }}
	flags.Init("", flag.ExitOnError)
	flags.BoolVar(&verbose, "v", verbose, "verbose mode")
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		prog.Exit(2)
	}
	storPath := argv[0]

	var d Dumper
	if verbose {
		d = &DumperFsDumpVerbose{}
	} else {
		d = &DumperFsDump{}
	}

	err := Dump(os.Stdout, storPath, fs1.IterForward, d)
	if err != nil {
		prog.Fatal(err)
	}
}

// ----------------------------------------

// DumperFsTail implements dumping with the same format as in fstail/py
// originally written by Jeremy Hylton:
//
//	https://github.com/zopefoundation/ZODB/blob/master/src/ZODB/scripts/fstail.py
//	https://github.com/zopefoundation/ZODB/commit/551122cc
type DumperFsTail struct {
	Ntxn int	// max # of transactions to dump
	data []byte	// buffer for reading txn data
}

func (d *DumperFsTail) DumperName() string {
	return "fstail"
}

func (d *DumperFsTail) DumpFileHeader(buf *xfmt.Buffer, fh *fs1.FileHeader) error {
	return nil
}

func (d *DumperFsTail) DumpTxn(buf *xfmt.Buffer, it *fs1.Iter) error {
	if d.Ntxn == 0 {
		return io.EOF
	}
	d.Ntxn--

	txnh := &it.Txnh

	// read raw data inside transaction record
	dataLen := txnh.DataLen()
	d.data = xbytes.Realloc64(d.data, dataLen)
	_, err := it.R.ReadAt(d.data, txnh.DataPos())
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF	// XXX -> noEOF(err)
		}
		return &fs1.TxnError{txnh.Pos, "read data payload", err}
	}

	// print information about read txn record
	dataSha1 := sha1.Sum(d.data)
	buf .V(txnh.Tid.Time()) .S(": hash=") .Xb(dataSha1[:])

	// fstail.py uses repr to print user/description:
	// https://github.com/zopefoundation/ZODB/blob/5.2.0-5-g6047e2fae/src/ZODB/scripts/fstail.py#L39
	buf .S("\nuser=") .Qpyb(txnh.User) .S(" description=") .Qpyb(txnh.Description)

	// XXX in zodb/py .length is len - 8, in zodb/go - whole txn record length	-> FIXME better .Len be what is on disk
	buf .S(" length=") .D64(txnh.Len - 8)
	buf .S(" offset=") .D64(txnh.Pos) .S(" (+") .D64(txnh.HeaderLen()) .S(")\n\n")

	return nil
}

const tailSummary = "dump last few transactions of a database"
const ntxnDefault = 10

func tailUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: fs1 tail [options] <storage>
Dump transactions from a FileStorage in reverse order

<storage> is a path to FileStorage

  options:

	-h --help       this help text.
	-n <N>	        output the last <N> transactions (default %d).
`, ntxnDefault)
}

func tailMain(argv []string) {
	ntxn := ntxnDefault

	flags := flag.FlagSet{Usage: func() { tailUsage(os.Stderr) }}
	flags.Init("", flag.ExitOnError)
	flags.IntVar(&ntxn, "n", ntxn, "output the last <N> transactions")
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		prog.Exit(2)
	}
	storPath := argv[0]

	err := Dump(os.Stdout, storPath, fs1.IterBackward, &DumperFsTail{Ntxn: ntxn})
	if err != nil {
		prog.Fatal(err)
	}
}
