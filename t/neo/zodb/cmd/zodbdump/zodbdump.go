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

Format
------

txn <tid> (<status>)
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

	"lab.nexedi.com/kirr/go123/mem"
)

func zodbDump(w io.Writer, stor zodb.IStorage, tidMin, tidMax zodb.Tid, hashOnly bool) error {
	iter := stor.Iterate(tidMin, tidMax)

	for {
		txni, dataIter, err := iter.NextTxn()
		if err != nil {
			err = okEOF(err)
			break
		}

		// TODO if not first: println

		_, err = fmt.Fprintf(w, "txn %s (%c)\nuser %q\ndescription %q\nextension %q\n",
				txni.Tid, txni.Status, txni.User, txni.Description, txni.Extension)
		if err != nil {
			break
		}


		for {
			datai, err := dataIter.NextData()
			if err != nil {
				err = okEOF(err)
				break
			}

			entry := "obj " + datai.Oid.String() + " "
			writeData := false

			switch {
			case datai.Data == nil:
				entry += "delete"

			case datai.Tid != datai.DataTid:
				entry += "from " + datai.DataTid.String()

			default:
				entry += fmt.Sprintf("sha1:%s %d", sha1.Sum(datai.Data), len(datai.Data))
				writeData = true
			}

			entry += "\n"
			_, err = w.Write(mem.Bytes(entry))
			if err != nil {
				break
			}

			if !hashOnly && writeData {
				_, err = w.Write(datai.Data)
				if err != nil {
					break
				}
			}

		}

		// XXX check err
	}

	// XXX check err
}

func usage() {
	fmt.Fprintf(os.Stderr,
`zodbdump [options] <command> [tidmin..tidmax]
Dump content of a ZODB database.

<storage> is a path to FileStorage	XXX will become URL

  options:

	-h --help       this help text.
	-hashonly	dump only hashes of objects without content.
`)
}

func main() {
	hashOnly := false
	tidMin := zodb.Tid(0)
	tidMax := zodb.TidMax

	flag.Usage = usage
	flag.BoolVar(&hashOnly, "hashonly", hashOnly, "dump only hashes of objects")
	flag.Parse()

	argv := flag.Args()
	if len(argv) < 1 {
		usage()
		os.Exit(2)	// XXX recheck it is same as from flag.Parse on -zzz
	}
	storUrl := argv[0]

	if len(argv) > 1 {
		tidMin, tidMax, err = parseTidRange(argv[1])
		if err != nil {
			log.Fatal(err)	// XXX recheck
		}
	}

	stor, err = fs1.Open(storUrl)	// TODO read-only
	if err != nil {
		log.Fatal(err)
	}

	err = zodbDump(os.Stdout, stor, tidMin, tidMax, hashOnly)
	if err != nil {
		log.Fatal(err)
	}
}
