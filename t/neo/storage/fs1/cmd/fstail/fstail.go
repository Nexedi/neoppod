// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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
fstail - Tool to dump the last few transactions from a FileStorage.

Format is the same as in fstail/py originally written by Jeremy Hylton:

	https://github.com/zopefoundation/ZODB/blob/master/src/ZODB/scripts/fstail.py
	https://github.com/zopefoundation/ZODB/commit/551122cc
*/

package main

import (
	"crypto/sha1"
	"flag"
)

fsDump(w io.Writer, path string, ntxn int) error {
	// we are not using fs1.Open here, since the file could be e.g. corrupt
	// (same logic as in fstail/py)
	f, err := os.Open(path)
	// XXX err

	// TODO get fSize

	txnh := fs1.TxnHeader{}

	// start iterating at tail.
	// this should get EOF but read txnh.LenPrev ok.
	err = txnh.Load(f, fSize, LoadAll)
	if err != io.EOF {
		if err == nil {
			// XXX or allow this?
			// though then, if we start not from a txn boundary - there are
			// high chances further iteration will go wrong.
			err = fmt.Errorf("%s @%v: reading past file end was unexpectedly successful: probably the file is being modified simultaneously",
				path, fSize)
		}
		// otherwise err already has the context
		return err
	}
	if txnh.LenPrev <= 0 {
		return fmt.Errorf("%s @%v: previous record could not be read", path, fSize)
	}

	// now loop loading previous txn until EOF / ntxn limit
	for {
		err = txnh.LoadPrev(fs1.LoadAll)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		// TODO read raw data	(get_raw_data) -> sha1
		// txnh.Tid.TimeStamp() ?  XXX -> PerTimeStamp ? (get_timestamp)

		//"user=%'q description=%'q length=%d offset=%d (+%d)", txnh.User, txnh.Description, // .Len, .offset ...
	}

	if err != nil {
		// TODO
	}
}

func usage() {
	fmt.Fprintf(os.Stderr,
`fstail [options] <storage>
Dump transactions from a FileStorage in reverse order

<storage> is a path to FileStorage

  options:

	-h --help       this help text.
	-n <N>	        output the last <N> transactions (default %d).
`)
}

func main() {
	ntxn := 10
	flag.IntVar(&ntxn, "n", ntxn, "output the last <N> transactions")
	flag.Parse()

	argv := flag.Args()
	if len(argv) < 1 {
		usage()
		os.Exit(2)	// XXX recheck it is same as from flag.Parse on -zzz
	}
	storPath := argv[0]

	err = fsDump(os.Stdout, storPath, n)
	if err != nil {
		log.Fatal(err)
	}
}
