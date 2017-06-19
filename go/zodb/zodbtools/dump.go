// Copyright (C) 2016-2017  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Open Source Initiative approved licenses and Convey
// the resulting work. Corresponding source of such a combination shall include
// the source code for all other software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

/*
Zodbdump - Tool to dump content of a ZODB database

TODO sync text with zodbdump/py

Format
------

txn <tid> (<status>)	XXX escape status ?
user <user|quote>
description <description|quote>
extension <extension|quote>
obj <oid> (delete | from <tid> | sha1:<sha1> <size> (LF <content>)?) LF     XXX do we really need back <tid>
---- // ----
LF
txn ...

*/

package zodbtools

import (
	"context"
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

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
// XXX naming -> DumpObj ?
func (d *dumper) DumpData(datai *zodb.StorageRecordInformation) error {
	buf := &d.buf
	buf.Reset()

	buf .S("obj ") .V(&datai.Oid) .Cb(' ')

	writeData := false

	switch {
	case datai.Data == nil:
		buf .S("delete")

	case datai.Tid != datai.DataTid:
		buf .S("from ") .V(&datai.DataTid)

	default:
		dataSha1 := sha1.Sum(datai.Data)
		buf .D(len(datai.Data)) .S(" sha1:") .Xb(dataSha1[:])

		writeData = true
	}

	buf .Cb('\n')

	// TODO use writev(data, "\n") via net.Buffers (it is already available)
	_, err := d.W.Write(buf.Bytes())
	if err != nil {
		goto out
	}

	if writeData && !d.HashOnly {
		_, err = d.W.Write(datai.Data)
		if err != nil {
			goto out
		}

		// XXX maybe better to merge with next record ?
		_, err = d.W.Write(_LF)
		if err != nil {
			goto out
		}
	}

out:
	// XXX do we need this context ?
	// see for rationale in similar place in DumpTxn
	if err != nil {
		return fmt.Errorf("%v: %v", datai.Oid, err)
	}

	return nil
}

// DumpTxn dumps one transaction record
func (d *dumper) DumpTxn(txni *zodb.TxnInfo, dataIter zodb.IStorageRecordIterator) error {
	var datai *zodb.StorageRecordInformation

	// LF in-between txn records
	vskip := "\n"
	if !d.afterFirst {
		vskip = ""
		d.afterFirst = true
	}

	_, err := fmt.Fprintf(d.W, "%stxn %s (%c)\nuser %q\ndescription %q\nextension %q\n",
			vskip, txni.Tid, txni.Status, txni.User, txni.Description, txni.Extension)
	if err != nil {
		goto out
	}

	// data records
	for {
		datai, err = dataIter.NextData()
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

out:
	// XXX do we need this context ?
	// rationale: dataIter.NextData() if error in db - will include db context
	// if error is in writer - it will include its own context
	if err != nil {
		return fmt.Errorf("%v: %v", txni.Tid, err)
	}

	return nil
}

// Dump dumps transaction records in between tidMin..tidMax
func (d *dumper) Dump(stor zodb.IStorage, tidMin, tidMax zodb.Tid) error {
	var txni     *zodb.TxnInfo
	var dataIter zodb.IStorageRecordIterator
	var err      error

	iter := stor.Iterate(tidMin, tidMax)

	// transactions
	for {
		txni, dataIter, err = iter.NextTxn()
		if err != nil {
			if err == io.EOF {
				err = nil	// XXX -> okEOF ?
			}

			break
		}

		err = d.DumpTxn(txni, dataIter)
		if err != nil {
			break
		}
	}

	if err != nil {
		return fmt.Errorf("%s: dumping %v..%v: %v", stor, tidMin, tidMax, err)
	}

	return nil
}

// Dump dumps contents of a storage in between tidMin..tidMax range to a writer.
// see top-level documentation for the dump format.
func Dump(w io.Writer, stor zodb.IStorage, tidMin, tidMax zodb.Tid, hashOnly bool) error {
	d := dumper{W: w, HashOnly: hashOnly}
	return d.Dump(stor, tidMin, tidMax)
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
		os.Exit(2)
	}
	storUrl := argv[0]

	if len(argv) > 1 {
		tidRange = argv[1]
	}

	tidMin, tidMax, err := zodb.ParseTidRange(tidRange)
	if err != nil {
		log.Fatal(err)	// XXX recheck
	}

	stor, err := zodb.OpenStorageURL(context.Background(), storUrl)	// TODO read-only
	if err != nil {
		log.Fatal(err)
	}
	// TODO defer stor.Close()

	err = Dump(os.Stdout, stor, tidMin, tidMax, hashOnly)
	if err != nil {
		log.Fatal(err)
	}
}
