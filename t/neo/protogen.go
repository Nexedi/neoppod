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
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"log"
	"os"
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
func pos(n ast.Node) token.Position {
	return fset.Position(n.Pos())
}

func main() {
	log.SetFlags(0)
	//typeMap := map[string]*PacketType{}	// XXX needed ?

	// go through proto.go and collect packets type definitions

	var mode parser.Mode = 0	// parser.Trace
	var fv []*ast.File
	for _, src := range []string{"proto.go", "neo.go"} {
		f, err := parser.ParseFile(fset, src, nil, mode)
		if err != nil {
			log.Fatalf("parse: %v", err)
		}
		fv = append(fv, f)
	}

	conf := types.Config{Importer: importer.Default()}
	_, err := conf.Check("neo", fset, fv, info)
	if err != nil {
		log.Fatalf("typecheck: %v", err)
	}


	//ncode := 0

	//ast.Print(fset, f)
	//return

	f := fv[0]	// proto.go comes first
	out := Buffer{}

	for _, decl := range f.Decls {
		// we look for types (which can be only under GenDecl)
		gendecl, ok := decl.(*ast.GenDecl)
		if !ok || gendecl.Tok != token.TYPE {
			continue
		}

		for _, spec := range gendecl.Specs {
			typespec := spec.(*ast.TypeSpec) // must be because tok = TYPE
			//typename := typespec.Name.Name

			switch typespec.Type.(type) {
			default:
				// we are only interested in struct types
				continue

			case *ast.StructType:
				//fmt.Printf("\n%s:\n", typename)
				//continue
				//fmt.Println(t)
				//ast.Print(fset, t)

				out.WriteString(gendecode(typespec))
				/*
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
				*/
			}
		}

		//fmt.Println(gdecl)
		//ast.Print(fset, gdecl)
	}

	// format & emit out
	outf, err := format.Source(out.Bytes())
	if err != nil {
		panic(err)	// should not happen
	}

	_, err = os.Stdout.Write(outf)
	//_, err = os.Stdout.Write(out.Bytes())
	if err != nil {
		log.Fatal(err)
	}
}

/*
// wiresize returns wire size of a type
// type must be of fixed size (e.g. not a slice or map)
// XXX ast.Expr -> ?
func wiresize(*ast.Expr) int {
	// TODO
}
*/

// info about wire decode/encode of a basic type
type basicXXX struct {
	wireSize int
	decode string
	//encode string
}

var basicDecode = map[types.BasicKind]basicXXX {
	// %v will be `data[n:n+wireSize]`	XXX or `data[n:]` ?
	types.Bool:	{1, "bool((%v)[0])"},
	types.Int8:	{1, "int8((%v)[0])"},
	types.Int16:	{2, "int16(BigEndian.Uint16(%v))"},
	types.Int32:	{4, "int32(BigEndian.Uint32(%v))"},
	types.Int64:	{8, "int64(BigEndian.Uint64(%v))"},

	types.Uint8:	{1, "(%v)[0]"},
	types.Uint16:	{2, "BigEndian.Uint16(%v)"},
	types.Uint32:	{4, "BigEndian.Uint32(%v)"},
	types.Uint64:	{8, "BigEndian.Uint64(%v)"},

	types.Float64:	{8, "float64_NEODecode(%v)"},

	// XXX string ?
}

// bytes.Buffer + bell&whistless
type Buffer struct {
	bytes.Buffer
}

func (b *Buffer) Printfln(format string, a ...interface{}) (n int, err error) {
	return fmt.Fprintf(b, format+"\n", a...)
}


func gendecode(typespec *ast.TypeSpec) string {
	buf := Buffer{}
	emit := buf.Printfln

	typename := typespec.Name.Name
	t := typespec.Type.(*ast.StructType)	// must be
	emit("func (p *%s) NEODecode(data []byte) (int, error) {", typename)

	n := 0	// current decode pos in data

	for _, fieldv := range t.Fields.List {
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

		// decode basic fixed types (not string)
		decodeBasic := func(typ *types.Basic) string {
			bdec, ok := basicDecode[typ.Kind()]
			if !ok {
				log.Fatalf("%v: basic type %v not supported", pos(fieldv), typ)
			}
			dataptr := fmt.Sprintf("data[%v:]", n)
			decoded := fmt.Sprintf(bdec.decode, dataptr)
			n += bdec.wireSize
			return decoded
		}

		emitstrbytes := func(fieldname string) {
			emit("{ l := %v", decodeBasic(types.Typ[types.Uint32]))
			emit("data = data[%v:]", n)
			emit("if len(data) < l { return 0, ErrDecodeOverflow }")
			emit("p.%v = string(data[:l])", fieldname)
			emit("data = data[l:]")
			emit("}")
			n = 0
		}


		for _, fieldname := range fieldnamev {
			fieldtype := info.Types[fieldv.Type].Type
			switch u := fieldtype.Underlying().(type) {
			// we are processing:	<fieldname> <fieldtype>

			// bool, uint32, string, ...
			case *types.Basic:
				if u.Kind() == types.String {
					emitstrbytes(fieldname.Name)
					continue
				}

				emit("p.%s = %s", fieldname, decodeBasic(u))

			case *types.Slice:
				// TODO

			case *types.Map:
				// TODO


			// TODO types.Struct



			/*

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
			*/

			default:
				log.Fatalf("%v: field %v has unsupported type %v", pos(fieldv), fieldname, fieldtype)
			}
		}
	}

	emit("return %v /* + TODO variable part */, nil", n)
	emit("}")
	return buf.String()
}
