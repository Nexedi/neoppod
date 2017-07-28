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
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"log"

	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"
)

// Reindex rebuilds index for FileStorage file @ path
func Reindex(ctx context.Context, path string) error {
	// XXX lock path.lock ?
	index, err := fs1.BuildIndexForFile(ctx, path)
	if err != nil {
		return err
	}

	err = index.SaveFile(path + ".index")
	if err != nil {
		return err // XXX err ctx
	}

	return nil
}

const reindexSummary = "rebuild database index"

func reindexUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: fs1 reindex [options] <storage>
Rebuild FileStorage index

<storage> is a path to FileStorage

  options:

	-h --help       this help text.
`)
}

func reindexMain(argv []string) {
	flags := flag.FlagSet{Usage: func() { reindexUsage(os.Stderr) }}
	flags.Init("", flag.ExitOnError)
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

// ----------------------------------------

// TODO verify-index -quick (only small sanity check)

// VerifyIndexFor verifies that on-disk index for FileStorage file @ path is correct
func VerifyIndexFor(ctx context.Context, path string) (err error) {
	// XXX lock path.lock ?
	inverify := false
	defer func() {
		if inverify {
			return // index.Verify provides all context in error
		}
		xerr.Contextf(&err, "%s: verify index", path)
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}

	defer func() {
		err2 := f.Close()	// XXX vs inverify
		err = xerr.First(err, err2)
	}()

	index, err := LoadIndexFile(path + ".index")
	if err != nil {
		return err
	}

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	topPos := fi.Size()	// XXX there might be last TxnInprogress transaction
	if index.TopPos != topPos {
		return fmt.Errorf("topPos mismatch: data=%v  index=%v", topPos, index.TopPos)
	}

	// XXX - better somehow not here?
	fSeq := xbufio.NewSeqReaderAt(f)

	inverify = true
	err = index.Verify(ctx, fSeq)
	return err
}

const verifyIdxSummary = "verify database index"

func verifyIdxUsage(w io.Writer) {
	panic("TODO")	// XXX
}

func verifyIdxMain(argv []string) {
	panic("TODO")	// XXX
}
