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
	"flag"
	"fmt"
	"io"
	"os"
	"log"
)

// Reindex rebuilds index for FileStorage file @ path
func Reindex(path string) error {
	// XXX open read-only
	fs, err := fs1.Open(contex.Background(), path, fs1.OpenWithoutIndex)
	if err != nil {
		return nil	// XXX err ctx
	}
	defer fs.Close()	// XXX err

	err = fs.Reindex()
	return err	// XXX ok?
}

// VerifyIndexFor verifies that on-disk index for FileStorage file @ path is correct
func VerifyIndexFor(path string) error {
	// XXX open read-only
	fs, err := fs1.Open(contex.Background(), path, 0)
	if err != nil {
		return nil	// XXX err ctx
	}
	defer fs.Close()	// XXX err

	err = fs.VerifyIndex()
	return err
	//fs.Index()
	//fs.ComputeIndex
}

// ----------------------------------------

const reindexSummary = "dump last few transactions of a database"

func reindexUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: fs1 reindex [options] <storage>
Dump transactions from a FileStorage in reverse order	XXX

<storage> is a path to FileStorage

  options:

	-h --help       this help text.
	-verify         verify that existing index is correct; don't overwrite it
`, ntxnDefault)
}

func reindexMain(argv []string) {
	flags := flag.FlagSet{Usage: func() { reindexUsage(os.Stderr) }}
	flags.Init("", flag.ExitOnError)
	// XXX -verify
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		os.Exit(2)
	}
	storPath := argv[0]

	err := Reindex(storPath)
	if err != nil {
		log.Fatal(err)
	}
}
