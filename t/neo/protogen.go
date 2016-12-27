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
	"go/types"
	"log"
)

// information about one packet type
type PacketType struct {
	name    string
	msgCode uint16 // message code for this packet type - derived from type order number in source
}

var fset = token.NewFileSet()
var info = &types.Info{
	Types: make(map[ast.Expr]types.TypeAndValue),
	Uses:  make(map[*ast.Ident]types.Object),
	Defs:  make(map[*ast.Ident]types.Object),
}

// complete position of a node
func pos(n ast.Node) {
	return fset.Position(n.Pos())
}

func main() {
	typeMap := map[string]*PacketType{}	// XXX needed ?

	// go through proto.go and collect packets type definitions

	var mode parser.Mode = 0	// parser.Trace
	f, err := parser.ParseFile(fset, "proto.go", nil, mode)
	if err != nil {
		log.Fatal(err)	// parse error
	}

	conf := types.Config{}
	pkg, err := conf.Check("proto", fset, []*ast.File{f}, info)
	if err != nil {
		log.Fatal(err)	// typecheck error
	}


	ncode := 0

	//ast.Print(fset, f)
	//return

	for _, decl := range f.Decls {
		// we look for types (which can be only under GenDecl)
		gendecl, ok := decl.(*ast.GenDecl)
		if !ok || gendecl.Tok != token.TYPE {
			continue
		}

		for _, spec := range gendecl.Specs {
			typespec := spec.(*ast.TypeSpec) // must be because tok = TYPE
			typename := typespec.Name.Name

			switch t := typespec.Type.(type) {
			default:
				// we are only interested in struct types
				continue

			case *ast.StructType:
				//fmt.Printf("\n%s:\n", typename)
				//fmt.Println(t)
				//ast.Print(fset, t)

				PacketType{name: typename, msgCode: ncode}

				// if ncode != 0 {
				// 	fmt.Println()
				// }

				for _, fieldv := range t.Fields.List {
					// we only support simple types like uint16
					ftype, ok := fieldv.Type.(*ast.Ident)
					if !ok {
						// TODO log
						// TODO proper error message
						panic(fmt.Sprintf("%#v not supported", fieldv.Type))
					}

					if len(fieldv.Names) != 0 {
						for _, field := range fieldv.Names {
							fmt.Printf("%s(%d).%s\t%s\n", typename, ncode, field.Name, ftype)
						}
					} else {
						// no names means embedding
						fmt.Printf("%s(%d).<%s>\n", typename, ncode, ftype)
					}
				}

				ncode++
			}
		}

		//fmt.Println(gdecl)
		//ast.Print(fset, gdecl)
	}
}

// wiresize returns wire size of a type
// type must be of fixed size (e.g. not a slice or map)
// XXX ast.Expr -> ?
func wiresize(*ast.Expr) int {
	// TODO
}

func gendecode(typespec *ast.TypeSpec) string {
	buf := butes.Buffer{}

	typename := typespec.Name.Name
	t := typespec.Type.(*ast.StructType)	// must be
	fmt.Fprintf(&buf, "func (p *%s) NEODecode(data []byte) int {\n", typename)

	n := 0	// current decode pos in data

	for _, fieldv := t.Fields.List {
		// type B struct { ... }
		//
		// type A struct {
		//	x, y int	<- fieldv
		//	B		<- fieldv

		// embedding: change `B` -> `B B` (field type must be Ident)
		fieldnamev := fieldv.Names
		if fieldnamev == nil {
			fieldnamev = []*ast.Ident{fieldv.Type.(*ast.Ident)}
		}

		for fieldname := range fieldnamev {
			switch fieldtype := fieldv.Type.(type) {
			// we are processing:	<fieldname> <fieldtype>

			// simple types like uint16
			case *ast.Ident:
				// TODO

			// array or slice
			case *ast.ArrayType:
				if fieldtype.Len != nil {
					log.Fatalf("%s: TODO arrays not suported", pos(fieldtype))
				}

				eltsize := wiresize(fieldtype.Elt)	// TODO

				// len	  u32
				// [len] items
				emit("length = Uint32(data[%s:])", n)
				n += 4
				emit("for ; length != 0; length-- {")
				emit("}")



			// map
			case *ast.MapType:
				// len	  u32
				// [len] key, value
				emit("length = Uint32(data[%s:])", n)
				n += 4

				keysize := wiresize(fieldtype.Key)
				valsize := wiresize(fieldtype.Value)

			// XXX *ast.StructType ?

			default:
				panic()	// TODO
			}
		}
	}

	fmt.Fprintf(&buf, "}\n")
	// TODO format.Source(buf.Bytes())	(XXX -> better at top-level for whole file)
	return buf.String()
}
