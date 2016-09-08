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
	f, err := parser.ParseFile(fset, "pkt.go", nil, mode)
	if err != nil {
		panic(err)	// XXX log
	}

	ncode := 0

	//ast.Print(fset, f)
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

			/*
			fmt.Printf("%s:\n", tname)
			fmt.Println(tstruct)
			ast.Print(fset, tstruct)
			*/

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

				for _, field := range fieldv.Names {
					fmt.Printf("%s(%d).%s\t%s\n", tname, ncode, field.Name, ftype)
				}
			}

			ncode++

		}

		//fmt.Println(gdecl)
		//ast.Print(fset, gdecl)
	}
}
