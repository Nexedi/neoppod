// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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

package zodbtools
// Catobj - dump content of a database object

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"../../zodb"
)

// Catobj dumps content of one ZODB object	XXX text
func Catobj(w io.Writer, stor zodb.IStorage, xid zodb.Xid) error {
	var objInfo zodb.StorageRecordInformation

	data, tid, err := stor.Load(xid)
	if err != nil {
		return err
	}

	// XXX hack - rework IStorage.Load to fill-in objInfo directly
	objInfo.Oid = xid.Oid
	objInfo.Tid = tid
	objInfo.Data = data
	objInfo.DataTid = tid	// XXX wrong

	d := dumper{W: w}	// XXX HashOnly + raw dump
	err = d.DumpData(&objInfo)
	return err
}

// ----------------------------------------
const catobjSummary = "dump content of a database object"

func catobjUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: zodb catobj [OPTIONS] <storage> xid...
Dump content of a ZODB database object.

<storage> is an URL (see 'zodb help zurl') of a ZODB-storage.
xid is object address (see 'zodb help xid').

Options:

	-h --help       this help text.
//	-hashonly	dump only hashes of objects without content.	XXX
`)
}

func catobjMain(argv []string) {
	flags := flag.FlagSet{Usage: func() { catobjUsage(os.Stderr) }}
	flags.Init("", flag.ExitOnError)
//	flags.BoolVar(&hashOnly, "hashonly", hashOnly, "dump only hashes of objects")
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 2 {
		flags.Usage()
		os.Exit(2)
	}
	storUrl := argv[0]

	xidv := []zodb.Xid{}
	for _, arg := range argv[1:] {
		xid, err := zodb.ParseXid(arg)
		if err != nil {
			log.Fatal(err)	// XXX recheck
		}

		xidv = append(xidv, xid)
	}

	stor, err := zodb.OpenStorageURL(storUrl)	// TODO read-only
	if err != nil {
		log.Fatal(err)
	}
	// TODO defer stor.Close()

	for _, xid := range xidv {
		err = Catobj(os.Stdout, stor, xid)
		if err != nil {
			log.Fatal(err)
		}
	}
}
