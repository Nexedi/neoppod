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

// complete position of something with .Pos()
func pos(x interface { Pos() token.Pos }) token.Position {
	return fset.Position(x.Pos())
}

func main() {
	var err error

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
	_, err = conf.Check("neo", fset, fv, info)
	if err != nil {
		log.Fatalf("typecheck: %v", err)
	}


	//ncode := 0

	//ast.Print(fset, f)
	//return

	f := fv[0]	// proto.go comes first
	buf := Buffer{}
	buf.WriteString("package neo\n")

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

				buf.WriteString(gendecode(typespec))
				buf.WriteString("\n")
			}
		}

		//fmt.Println(gdecl)
		//ast.Print(fset, gdecl)
	}

	// format & emit bufferred code
	code, err := format.Source(buf.Bytes())
	if err != nil {
		panic(err)	// should not happen
	}

	_, err = os.Stdout.Write(code)
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


// state of decode codegen
type decoder struct {
	buf Buffer	// buffer for generated code
	n int		// current decode position in data
}

func (d *decoder) emit(format string, a ...interface{}) {
	fmt.Fprintf(&d.buf, format+"\n", a...)
}

// emit code for decode basic fixed types (not string), but do not assign it
func (d *decoder) decodedBasic (obj types.Object, typ *types.Basic) string {
	bdec, ok := basicDecode[typ.Kind()]
	if !ok {
		log.Fatalf("%v: %v: basic type %v not supported", pos(obj), obj.Name(), typ)
	}
	dataptr := fmt.Sprintf("data[%v:]", d.n)
	decoded := fmt.Sprintf(bdec.decode, dataptr)
	d.n += bdec.wireSize
	return decoded
}

// emit code for decode next string or []byte
func (d *decoder) emitstrbytes(assignto string) {
	// len	u32
	// [len]byte
	d.emit("{ l := %v", d.decodedBasic(nil, types.Typ[types.Uint32]))
	d.emit("data = data[%v:]", d.n)
	d.emit("if len(data) < l { return 0, ErrDecodeOverflow }")
	d.emit("%v = string(data[:l])", assignto)
	d.emit("data = data[l:]")
	d.emit("}")
	d.n = 0
}

func (d *decoder) emitslice(assignto string, obj types.Object, typ *types.Slice) {
	// len	u32
	// [len]item
	d.emit("{ l := %v", d.decodedBasic(nil, types.Typ[types.Uint32]))
	d.emit("data = data[%v:]", d.n)
	d.n = 0
	d.emit("%v = make(%v, l)", assignto, typ)
	// TODO size check
	// TODO if size(item)==const - check l in one go
	//d.emit("if len(data) < l { return 0, ErrDecodeOverflow }")
	d.emit("for i := 0; i < l; i++ {")
	d.emit("a := &%s[i]", assignto)
	d.emitobjtype("a", obj, typ.Elem())	// XXX also obj.Elem() ?
	d.emit("data = data[%v:]", d.n)	// FIXME wrt slice of slice ?
	d.emit("}")
	//d.emit("%v = string(data[:l])", assignto)
	d.emit("}")
	d.n = 0
}

func (d *decoder) emitmap(assignto string, obj types.Object, typ *types.Map) {
	// len  u32
	// [len](key, value)
	d.emit("{ l := %v", d.decodedBasic(nil, types.Typ[types.Uint32]))
	d.emit("data = data[%v:]", d.n)
	d.n = 0
	d.emit("%v = make(%v, l)", assignto, typ)
	// TODO size check
	// TODO if size(item)==const - check l in one go
	//d.emit("if len(data) < l { return 0, ErrDecodeOverflow }")
	d.emit("m := %v", assignto)
	d.emit("for i := 0; i < l; i++ {")
	d.emitobjtype("key", obj, typ.Key())	// TODO -> :=
	d.emitobjtype("m[key]", obj, typ.Elem())
	d.emit("data = data[%v:]", d.n)	// FIXME wrt map of map ?
	d.emit("}")
	//d.emit("%v = string(data[:l])", assignto)
	d.emit("}")
	d.n = 0
}

// top-level driver for emitting decode code for obj/type
func (d *decoder) emitobjtype(assignto string, obj types.Object, typ types.Type) {
	switch u := typ.Underlying().(type) {
	case *types.Basic:
		if u.Kind() == types.String {
			d.emitstrbytes(assignto)
			break
		}

		d.emit("%s = %s", assignto, d.decodedBasic(obj, u))

	case *types.Slice:
		d.emitslice(assignto, obj, u)

	case *types.Map:
		d.emitmap(assignto, obj, u)


	case *types.Struct:
		for i := 0; i < u.NumFields(); i++ {
			v := u.Field(i)
			d.emitobjtype(assignto + "." + v.Name(), v, v.Type())
		}

	default:
		log.Fatalf("%v: %v has unsupported type %v (%v)", pos(obj),
			obj.Name(), typ, u)
	}
}


// generate decoder func for a type declaration typespec
func gendecode(typespec *ast.TypeSpec) string {
	d := decoder{}
	// prologue
	d.emit("func (p *%s) NEODecode(data []byte) (int, error) {", typespec.Name.Name)

	//n := 0
	//t := typespec.Type.(*ast.StructType)	// must be

	// type & object which refers to this type
	typ := info.Types[typespec.Type].Type
	obj := info.Defs[typespec.Name]

	d.emitobjtype("p", obj, typ)

	d.emit("return %v /* + TODO variable part */, nil", d.n)	// FIXME n is wrong after reset
	d.emit("}")
	return d.buf.String()


//	for _, fieldv := range t.Fields.List {
//		// type B struct { ... }
//		//
//		// type A struct {
//		//	x, y int	<- fieldv
//		//	B		<- fieldv
//
//		// embedding: change `B` -> `B B` (field type must be Ident)
//		fieldnamev := fieldv.Names
//		if fieldnamev == nil {
//			fieldnamev = []*ast.Ident{fieldv.Type.(*ast.Ident)}
//		}
//
//
//		for _, fieldname := range fieldnamev {
//			fieldtype := info.Types[fieldv.Type].Type
//
//			switch u := fieldtype.Underlying().(type) {
//			// we are processing:	<fieldname> <fieldtype>
//
//			// bool, uint32, string, ...
//			case *types.Basic:
//				if u.Kind() == types.String {
//					emitstrbytes(fieldname.Name)
//					continue
//				}
//
//				emit("p.%s = %s", fieldname, decodeBasic(u))
//
//			case *types.Slice:
//				// TODO
//
//			case *types.Map:
//				// TODO
//
//
//			//case *types.Struct:
//				// TODO
//
//
//
//			/*
//
//			// simple types like uint16
//			case *ast.Ident:
//				// TODO
//
//			// array or slice
//			case *ast.ArrayType:
//				if fieldtype.Len != nil {
//					log.Fatalf("%s: TODO arrays not suported", pos(fieldtype))
//				}
//
//				eltsize := wiresize(fieldtype.Elt)	// TODO
//
//				// len	  u32
//				// [len] items
//				emit("length = Uint32(data[%s:])", n)
//				n += 4
//				emit("for ; length != 0; length-- {")
//				emit("}")
//
//
//
//			// map
//			case *ast.MapType:
//				// len	  u32
//				// [len] key, value
//				emit("length = Uint32(data[%s:])", n)
//				n += 4
//
//				keysize := wiresize(fieldtype.Key)
//				valsize := wiresize(fieldtype.Value)
//
//			// XXX *ast.StructType ?
//			*/
//
//			default:
//				log.Fatalf("%v: field %v has unsupported type %v (%v)", pos(fieldv),
//					fieldname, fieldtype, u)
//			}
//		}
//	}

}
