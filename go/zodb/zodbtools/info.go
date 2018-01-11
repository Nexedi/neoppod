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

// Zodbinfo - Print general information about a ZODB database

package zodbtools

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"lab.nexedi.com/kirr/go123/prog"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

// paramFunc is a function to retrieve 1 storage parameter
type paramFunc func(ctx context.Context, stor zodb.IStorage) (string, error)

var infov = []struct {name string; getParam paramFunc} {
	{"name", func(ctx context.Context, stor zodb.IStorage) (string, error) {
		return stor.URL(), nil
	}},
// TODO reenable size
//	{"size", func(stor zodb.IStorage) (string, error) { return stor.StorageSize(), nil }},
	{"last_tid", func(ctx context.Context, stor zodb.IStorage) (string, error) {
		tid, err := stor.LastTid(ctx)
		return tid.String(), err
	}},
}

// {} parameter_name -> get_parameter(stor)
var infoDict = map[string]paramFunc{}

func init() {
	for _, info := range infov {
		infoDict[info.name] = info.getParam
	}
}

// Info prints general information about a ZODB storage
func Info(ctx context.Context, w io.Writer, stor zodb.IStorage, parameterv []string) error {
	wantnames := false
	if len(parameterv) == 0 {
		for _, info := range infov {
			parameterv = append(parameterv, info.name)
		}
		wantnames = true
	}

	for _, parameter := range parameterv {
		getParam, ok := infoDict[parameter]
		if !ok {
			return fmt.Errorf("invalid parameter: %s", parameter)
		}

		out := ""
		if wantnames {
			out += parameter + "="
		}
		value, err := getParam(ctx, stor)
		if err != nil {
			return fmt.Errorf("getting %s: %v", parameter, err)
		}
		out += value
		fmt.Fprintf(w, "%s\n", out)
	}

	return nil
}

// ----------------------------------------

const infoSummary = "print general information about a ZODB database"

func infoUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: zodb info [OPTIONS] <storage> [parameter ...]
Print general information about a ZODB database.

<storage> is an URL (see 'zodb help zurl') of a ZODB-storage.

By default info prints information about all storage parameters. If one or
more parameter names are given as arguments, info prints the value of each
named parameter on its own line.

Options:

    -h  --help      show this help
`)
}

func infoMain(argv []string) {
	flags := flag.FlagSet{Usage: func() { infoUsage(os.Stderr) }}
	flags.Init("", flag.ExitOnError)
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		prog.Exit(2)
	}
	storUrl := argv[0]

	ctx := context.Background()

	stor, err := zodb.OpenStorage(ctx, storUrl, &zodb.OpenOptions{ReadOnly: true})
	if err != nil {
		prog.Fatal(err)
	}
	// TODO defer stor.Close()

	err = Info(ctx, os.Stdout, stor, argv[1:])
	if err != nil {
		prog.Fatal(err)
	}
}
