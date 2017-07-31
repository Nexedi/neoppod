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

	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"
	zt "lab.nexedi.com/kirr/neo/go/zodb/zodbtools"
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
		zt.Exit(2)
	}
	storPath := argv[0]

	err := Reindex(context.Background(), storPath)
	if err != nil {
		zt.Fatal(err)
	}
}

// ----------------------------------------

// VerifyIndexFor verifies that on-disk index for FileStorage file @ path is correct
func VerifyIndexFor(ctx context.Context, path string, ntxn int, progress func(*fs1.IndexVerifyProgress)) (err error) {
	// XXX lock path.lock ?
	index, err := fs1.LoadIndexFile(path + ".index")
	if err != nil {
		return err	// XXX err ctx
	}

	_, err = index.VerifyForFile(context.Background(), path, ntxn, progress)
	return err
}

const verifyIdxSummary = "verify database index"

func verifyIdxUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: fs1 verify-index [options] <storage>
Verify FileStorage index

<storage> is a path to FileStorage

  options:

	-checkonly <n>	only check consistency by verifying against <n>
			last transactions.
	-quiet		do not show intermediate progress.
	-h --help       this help text.
`)
}

func verifyIdxMain(argv []string) {
	ntxn := -1
	quiet := false
	flags := flag.FlagSet{Usage: func() { verifyIdxUsage(os.Stderr) }}
	flags.Init("", flag.ExitOnError)
	flags.IntVar(&ntxn, "checkonly", ntxn, "check consistency only wrt last <n> transactions")
	flags.BoolVar(&quiet, "quiet", quiet, "do not show intermediate progress")
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		zt.Exit(2)
	}
	storPath := argv[0]

	// progress display
	progress := func(p *fs1.IndexVerifyProgress) {
		if p.TxnTotal == -1 {
			bytesChecked := p.Index.TopPos - p.Iter.Txnh.Pos
			bytesAll := p.Index.TopPos
			fmt.Printf("Checked data bytes: %.1f%% (%d/%d);  #txn: %d, #oid: %d\n",
				100 * float64(bytesChecked) / float64(bytesAll), // XXX /0 ?
				bytesChecked, bytesAll,
				p.TxnChecked, len(p.OidChecked))
		} else {
			fmt.Printf("Checked data transactions: %.1f%% (%d/%d);  #oid: %d\n",
				100 * float64(p.TxnChecked) / float64(p.TxnTotal), // XXX /0 ?
				p.TxnChecked, p.TxnTotal, len(p.OidChecked))
		}
	}


	if quiet {
		progress = nil
	}

	err := VerifyIndexFor(context.Background(), storPath, ntxn, progress)
	if err != nil {
		zt.Fatal(err)
	}

	if !quiet {
		fmt.Println("OK")
	}
}
