// Copyright (C) 2016  Nexedi SA and Contributors.
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

// NEO. Protocol definition. Code generator
// TODO text what it does (generates code for proto.go)

// +build ignore

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
)

var _ = ast.Print

func main() {
	fset := token.NewFileSet()

	var mode parser.Mode = 0	// parser.Trace
	f, err := parser.ParseFile(fset, "proto.go", nil, mode)
	if err != nil {
		panic(err)	// XXX log
	}

	ncode := 0

	//ast.Print(fset, f)
	//return

	for _, decl := range f.Decls {
		// we look for types (which can be only under GenDecl)
		gdecl, ok := decl.(*ast.GenDecl)
		if !ok || gdecl.Tok != token.TYPE {
			continue
		}

		for _, spec := range gdecl.Specs {
			tspec := spec.(*ast.TypeSpec)	// must be because tok = TYPE
			tname := tspec.Name.Name

			// we are only interested in struct types
			tstruct, ok := tspec.Type.(*ast.StructType)
			if !ok {
				continue
			}

			//fmt.Printf("\n%s:\n", tname)
			//fmt.Println(tstruct)
			//ast.Print(fset, tstruct)

			if ncode != 0 {
				fmt.Println()
			}

			for _, fieldv := range tstruct.Fields.List {
				// we only support simple types like uint16
				ftype, ok := fieldv.Type.(*ast.Ident)
				if !ok {
					// TODO log
					// TODO proper error message
					panic(fmt.Sprintf("%#v not supported", fieldv.Type))
				}

				if len(fieldv.Names) != 0 {
					for _, field := range fieldv.Names {
						fmt.Printf("%s(%d).%s\t%s\n", tname, ncode, field.Name, ftype)
					}
				} else {
					// no names means embedding
					fmt.Printf("%s(%d).<%s>\n", tname, ncode, ftype)
				}
			}

			ncode++

		}

		//fmt.Println(gdecl)
		//ast.Print(fset, gdecl)
	}
}
