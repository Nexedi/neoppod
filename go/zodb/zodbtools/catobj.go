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

package zodbtools
// Catobj - dump content of a database object

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"lab.nexedi.com/kirr/go123/prog"
	"lab.nexedi.com/kirr/neo/go/zodb"
)


// Catobj dumps content of one ZODB object
// The object is printed in raw form without any headers (see Dumpobj)
func Catobj(ctx context.Context, w io.Writer, stor zodb.IStorage, xid zodb.Xid) error {
	buf, _, err := stor.Load(ctx, xid)
	if err != nil {
		return err
	}

	_, err = w.Write(buf.Data)	// NOTE deleted data are returned as err by Load
	buf.Release()
	return err		// XXX err ctx ?
}

// Dumpobj dumps content of one ZODB object with zodbdump-like header
func Dumpobj(ctx context.Context, w io.Writer, stor zodb.IStorage, xid zodb.Xid, hashOnly bool) error {
	var objInfo zodb.DataInfo

	buf, tid, err := stor.Load(ctx, xid)
	if err != nil {
		return err
	}

	// XXX hack - TODO rework IStorage.Load to fill-in objInfo directly
	objInfo.Oid = xid.Oid
	objInfo.Tid = tid
	objInfo.Data = buf.Data
	objInfo.DataTid = tid	// XXX generally wrong

	d := dumper{W: w, HashOnly: hashOnly}
	err = d.DumpData(&objInfo)
	buf.Release()
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
	-hashonly	dump only hashes of objects without content.
	-raw		dump object data without any headers. Only one object allowed.
`)
}

func catobjMain(argv []string) {
	hashOnly := false
	raw := false

	flags := flag.FlagSet{Usage: func() { catobjUsage(os.Stderr) }}
	flags.Init("", flag.ExitOnError)
	flags.BoolVar(&hashOnly, "hashonly", hashOnly, "dump only hashes of objects")
	flags.BoolVar(&raw, "raw", hashOnly, "dump object data without any headers. Only one object allowed.")
	// TODO also -batch to serve objects a-la `git cat-file --batch` ?
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 2 {
		flags.Usage()
		prog.Exit(2)
	}
	storUrl := argv[0]

	if hashOnly && raw {
		prog.Fatal("-hashonly & -raw are incompatible")
	}

	xidv := []zodb.Xid{}
	for _, arg := range argv[1:] {
		xid, err := zodb.ParseXid(arg)
		if err != nil {
			prog.Fatal(err)	// XXX recheck
		}

		xidv = append(xidv, xid)
	}

	if raw && len(xidv) > 1 {
		prog.Fatal("only 1 object allowed with -raw")
	}

	ctx := context.Background()

	stor, err := zodb.OpenStorageURL(ctx, storUrl)	// TODO read-only
	if err != nil {
		prog.Fatal(err)
	}
	// TODO defer stor.Close()

	catobj := func(xid zodb.Xid) error {
		if raw {
			return Catobj(ctx, os.Stdout, stor, xid)
		} else {
			return Dumpobj(ctx, os.Stdout, stor, xid, hashOnly)
		}
	}

	for _, xid := range xidv {
		err = catobj(xid)
		if err != nil {
			prog.Fatal(err)
		}
	}
}
