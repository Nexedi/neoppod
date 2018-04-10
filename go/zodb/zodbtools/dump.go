// Copyright (C) 2016-2017  Nexedi SA and Contributors.
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

/*
Zodbdump - Tool to dump content of a ZODB database

This program dumps content of a ZODB database.
It uses ZODB Storage iteration API to get list of transactions and for every
transaction prints transaction's header and information about changed objects.

The information dumped is complete raw information as stored in ZODB storage
and should be suitable for restoring the database from the dump file bit-to-bit
identical to its original. It is dumped in semi text-binary format where
object data is output as raw binary and everything else is text.

There is also shortened mode activated via -hashonly where only hash of object
data is printed without content.

Dump format:

    txn <tid> <status|quote>
    user <user|quote>
    description <description|quote>
    extension <extension|quote>
    obj <oid> (delete | from <tid> | <size> <hashfunc>:<hash> (-|LF <raw-content>)) LF
    obj ...
    ...
    obj ...
    LF
    txn ...

quote:      quote string with " with non-printable and control characters \-escaped
hashfunc:   one of sha1, sha256, sha512 ...

TODO also protect txn record by hash.
*/

package zodbtools

import (
	"context"
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"os"

	"lab.nexedi.com/kirr/go123/prog"
	"lab.nexedi.com/kirr/go123/xfmt"
	"lab.nexedi.com/kirr/neo/go/zodb"
)


// dumper dumps zodb record to a writer
type dumper struct {
	W          io.Writer
	HashOnly   bool		// whether to dump only hashes of data without content

	afterFirst bool // true after first transaction has been dumped

	buf xfmt.Buffer // reusable data buffer for formatting
}

var _LF = []byte{'\n'}


// DumpData dumps one data record
func (d *dumper) DumpData(datai *zodb.DataInfo) (err error) {
	// XXX do we need this context ?
	// see for rationale in similar place in DumpTxn
	defer func() {
		if err != nil {
			err = fmt.Errorf("%v: %v", datai.Oid, err)
		}
	}()

	buf := &d.buf
	buf.Reset()

	buf .S("obj ") .V(&datai.Oid) .Cb(' ')

	writeData := false

	switch {
	case datai.Data == nil:
		buf .S("delete")

	case datai.DataTidHint != 0:
		buf .S("from ") .V(&datai.DataTidHint)

	default:
		// XXX sha1 is hardcoded for now. Dump format allows other hashes.
		dataSha1 := sha1.Sum(datai.Data)
		buf .D(len(datai.Data)) .S(" sha1:") .Xb(dataSha1[:])

		writeData = true
	}

	var data []byte
	if writeData {
		if d.HashOnly {
			buf .S(" -")
		} else {
			buf .Cb('\n')
			data = datai.Data
		}
	}

	// TODO use writev(buf, data, "\n") via net.Buffers (it is already available)
	_, err = d.W.Write(buf.Bytes())
	if err != nil {
		return err
	}

	if data != nil {
		_, err = d.W.Write(datai.Data)
		if err != nil {
			return err
		}
	}

	_, err = d.W.Write(_LF)
	return err
}

// DumpTxn dumps one transaction record
func (d *dumper) DumpTxn(ctx context.Context, txni *zodb.TxnInfo, dataIter zodb.IDataIterator) (err error) {
	// XXX do we need this context ?
	// rationale: dataIter.NextData() if error in db - will include db context
	// if error is in writer - it will include its own context
	defer func() {
		if err != nil {
			err = fmt.Errorf("%v: %v", txni.Tid, err)
		}
	}()

	var datai *zodb.DataInfo

	// LF in-between txn records
	vskip := "\n"
	if !d.afterFirst {
		vskip = ""
		d.afterFirst = true
	}

	_, err = fmt.Fprintf(d.W, "%stxn %s %q\nuser %q\ndescription %q\nextension %q\n",
			vskip, txni.Tid, string(txni.Status), txni.User, txni.Description, txni.Extension)
	if err != nil {
		return err
	}

	// data records
	for {
		datai, err = dataIter.NextData(ctx)
		if err != nil {
			if err == io.EOF {
				err = nil	// XXX -> okEOF ?
			}

			break
		}

		err = d.DumpData(datai)
		if err != nil {
			break
		}
	}

	return err
}

// Dump dumps transaction records in between tidMin..tidMax
func (d *dumper) Dump(ctx context.Context, stor zodb.IStorage, tidMin, tidMax zodb.Tid) error {
	var txni     *zodb.TxnInfo
	var dataIter zodb.IDataIterator
	var err      error

	iter := stor.Iterate(ctx, tidMin, tidMax)

	// transactions
	for {
		txni, dataIter, err = iter.NextTxn(ctx)
		if err != nil {
			if err == io.EOF {
				err = nil	// XXX -> okEOF ?
			}

			break
		}

		err = d.DumpTxn(ctx, txni, dataIter)
		if err != nil {
			break
		}
	}

	if err != nil {
		return fmt.Errorf("%s: dump %v..%v: %v", stor.URL(), tidMin, tidMax, err)
	}

	return nil
}

// Dump dumps contents of a storage in between tidMin..tidMax range to a writer.
//
// see top-level documentation for the dump format.
func Dump(ctx context.Context, w io.Writer, stor zodb.IStorage, tidMin, tidMax zodb.Tid, hashOnly bool) error {
	d := dumper{W: w, HashOnly: hashOnly}
	return d.Dump(ctx, stor, tidMin, tidMax)
}

// ----------------------------------------

const dumpSummary = "dump content of a ZODB database"

func dumpUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: zodb dump [OPTIONS] <storage> [tidmin..tidmax]
Dump content of a ZODB database.

<storage> is an URL (see 'zodb help zurl') of a ZODB-storage.

Options:

	-h --help       this help text.
	-hashonly	dump only hashes of objects without content.
`)
}

func dumpMain(argv []string) {
	hashOnly := false
	tidRange := ".." // [0, +inf]

	flags := flag.FlagSet{Usage: func() { dumpUsage(os.Stderr) }}
	flags.Init("", flag.ExitOnError)
	flags.BoolVar(&hashOnly, "hashonly", hashOnly, "dump only hashes of objects")
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		prog.Exit(2)
	}
	storUrl := argv[0]

	if len(argv) > 1 {
		tidRange = argv[1]
	}

	tidMin, tidMax, err := zodb.ParseTidRange(tidRange)
	if err != nil {
		prog.Fatal(err)
	}

	ctx := context.Background()

	stor, err := zodb.OpenStorage(ctx, storUrl, &zodb.OpenOptions{ReadOnly: true})
	if err != nil {
		prog.Fatal(err)
	}
	// TODO defer stor.Close()

	err = Dump(ctx, os.Stdout, stor, tidMin, tidMax, hashOnly)
	if err != nil {
		prog.Fatal(err)
	}
}
