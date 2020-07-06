// Copyright (C) 2019-2020  Nexedi SA and Contributors.
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

// Zodbwatch - watch ZODB database for changes
//
// Zodbwatch watches database for changes and prints information about
// committed transactions. Output formats:
//
// Plain:
//
//	# at <tid>
//	txn <tid>
//	txn <tid>
//	...
//
// Verbose:
//
//	# at <tid>
//	txn <tid>
//	obj <oid>
//	obj ...
//	...
//	LF
//	txn <tid>
//	...
//
// TODO add support for emitting transaction in zodbdump format.

package zodbtools

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"lab.nexedi.com/kirr/go123/prog"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

// Watch watches for database changes and prints them to w.
//
// see top-level documentation for output format.
func Watch(ctx context.Context, stor zodb.IStorage, w io.Writer, verbose bool) (err error) {
	defer xerr.Contextf(&err, "%s: watch", stor.URL())

	emitf := func(format string, argv ...interface{}) error {
		_, err := fmt.Fprintf(w, format, argv...)
		return err
	}

	watchq := make(chan zodb.Event)
	at0 := stor.AddWatch(watchq)
	defer stor.DelWatch(watchq)

	err = emitf("# at %s\n", at0)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event, ok := <-watchq:
			if !ok {
				err = emitf("# storage closed")
				return err
			}

			var δ *zodb.EventCommit
			switch event := event.(type) {
			default:
				panic(fmt.Sprintf("unexpected event: %T", event))

			case *zodb.EventError:
				return event.Err

			case *zodb.EventCommit:
				δ = event
			}

			err = emitf("txn %s\n", δ.Tid)
			if err != nil {
				return err
			}

			if verbose {
				for _, oid := range δ.Changev {
					err = emitf("obj %s\n", oid)
					if err != nil {
						return err
					}
				}

				err = emitf("\n")
				if err != nil {
					return err
				}
			}
		}
	}
}

// ----------------------------------------
const watchSummary = "watch ZODB database for changes"

func watchUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: zodb watch [OPTIONS] <storage>
Watch ZODB database for changes.

<storage> is an URL (see 'zodb help zurl') of a ZODB-storage.

Options:

	-h --help       this help text.
	-v		verbose mode.
`)
}

func watchMain(argv []string) {
	verbose := false
	flags := flag.FlagSet{Usage: func() { watchUsage(os.Stderr) }}
	flags.Init("", flag.ExitOnError)
	flags.BoolVar(&verbose, "v", verbose, "verbose mode")
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) != 1 {
		flags.Usage()
		prog.Exit(2)
	}
	zurl := argv[0]

	ctx := context.Background()
	err := func() (err error) {
		stor, err := zodb.Open(ctx, zurl, &zodb.OpenOptions{ReadOnly: true})
		if err != nil {
			return err
		}
		defer func() {
			err2 := stor.Close()
			if err == nil {
				err = err2
			}
		}()

		return Watch(ctx, stor, os.Stdout, verbose)
	}()

	if err != nil {
		prog.Fatal(err)
	}
}
