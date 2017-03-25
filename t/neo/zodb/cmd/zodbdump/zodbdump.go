// Copyright (C) 2016-2017  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 2, or (at your
// option) any later version, as published by the Free Software Foundation.
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

package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"../../../zodb"
	"../../../storage/fs1"
	"../../../xcommon/xfmt"
)


// dumper dumps zodb record to a writer
type dumper struct {
	W          io.Writer
	HashOnly   bool		// whether to dump only hashes of data without content

	afterFirst bool // true after first transaction has been dumped

	xbuf xfmt.Buffer // reusable data buffer for formatting
}

var _LF = []byte{'\n'}


// DumpData dumps one data record
func (d *dumper) DumpData(datai *zodb.StorageRecordInformation) error {
	xbuf := &d.xbuf
	xbuf.Reset()

	//entry := "obj " + datai.Oid.String() + " "
	xbuf .S("obj ") .V(&datai.Oid) .Cb(' ')

	writeData := false

	switch {
	case datai.Data == nil:
		//entry += "delete"
		xbuf.S("delete")

	case datai.Tid != datai.DataTid:
		//entry += "from " + datai.DataTid.String()
		xbuf .S("from ") .V(&datai.DataTid)

	default:
		//entry += fmt.Sprintf("%d sha1:%x", len(datai.Data), sha1.Sum(datai.Data))
		dataSha1 := sha1.Sum(datai.Data)
		xbuf .D(len(datai.Data)) .S(" sha1:") .Xb(dataSha1[:])

		writeData = true
	}

	//entry += "\n"
	xbuf .Cb('\n')

	// TODO use writev(data, "\n") via net.Buffers (it is already available)
	_, err := d.W.Write(xbuf.Bytes())
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

// zodbDump dumps contents of a storage in between tidMin..tidMax range to a writer.
// see top-level documentation for the dump format.
func zodbDump(w io.Writer, stor zodb.IStorage, tidMin, tidMax zodb.Tid, hashOnly bool) error {
	d := dumper{W: w, HashOnly: hashOnly}
	return d.Dump(stor, tidMin, tidMax)
}


func usage() {
	fmt.Fprintf(os.Stderr,
`zodbdump [options] <storage> [tidmin..tidmax]
Dump content of a ZODB database.

<storage> is a path to FileStorage	XXX will become URL

  options:

	-h --help       this help text.
	-hashonly	dump only hashes of objects without content.
`)
}

func main() {
	hashOnly := false
	tidRange := ".." // (0, +inf)

	flag.Usage = usage
	flag.BoolVar(&hashOnly, "hashonly", hashOnly, "dump only hashes of objects")
	flag.Parse()

	argv := flag.Args()
	if len(argv) < 1 {
		usage()
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

	stor, err := fs1.Open(storUrl)	// TODO read-only
	if err != nil {
		log.Fatal(err)
	}

	err = zodbDump(os.Stdout, stor, tidMin, tidMax, hashOnly)
	if err != nil {
		log.Fatal(err)
	}
}
