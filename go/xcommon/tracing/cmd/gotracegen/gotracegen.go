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

/*
gotracegen generates code according to tracing annotations and imports

gotracegen package

XXX tracepoints this package defines
XXX tracepoints this package imports
*/
package main

import (
	"flag"
	"fmt"
	"go/parser"
	"log"
	"os"

	"golang.org/x/tools/go/loader"
)

func tracegen(pkgpath string) error {
	conf := loader.Config{ParserMode: parser.ParseComments}
	conf.Import(pkgpath)

	// load package + all its imports
	lprog, err := conf.Load()
	if err != nil {
		log.Fatal(err)
	}

	// go through files of the package and process //trace: directives
	for _, file := range lprog.Package(pkgpath).Files {
		for _, commgroup := range file.Comments {
			for _, comment := range commgroup.List {
				fmt.Println(comment)	// .Slash .Text
			}
		}
	}

	return nil	// XXX
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
`gotracegen [options] [package]
TODO ...
`)
	}

	flag.Parse()

	argv := flag.Args()
	if len(argv) < 1 {
		flag.Usage()
		os.Exit(2)
	}
	pkgpath := argv[0]

	err := tracegen(pkgpath)
	if err != nil {
		log.Fatal(err)
	}
}
