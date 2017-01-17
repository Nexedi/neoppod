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
// TODO text what it does (generates marshal code for proto.go)

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

// parsed & typechecked input
var fset = token.NewFileSet()
var info = &types.Info{
	Types: make(map[ast.Expr]types.TypeAndValue),
	//Uses:  make(map[*ast.Ident]types.Object),	XXX seems not needed
	Defs:  make(map[*ast.Ident]types.Object),
}

// complete position of something with .Pos()
func pos(x interface { Pos() token.Pos }) token.Position {
	return fset.Position(x.Pos())
}

// get type name relative to neo package
var neoQualifier types.Qualifier
func typeName(typ types.Type) string {
	return types.TypeString(typ, neoQualifier)
}

func main() {
	var err error

	log.SetFlags(0)

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
	neoPkg, err := conf.Check("neo", fset, fv, info)
	if err != nil {
		log.Fatalf("typecheck: %v", err)
	}
	neoQualifier = types.RelativeTo(neoPkg)


	//ncode := 0

	//ast.Print(fset, f)
	//return

	// prologue
	f := fv[0]	// proto.go comes first
	buf := bytes.Buffer{}
	buf.WriteString(`// DO NOT EDIT - AUTOGENERATED (by protogen.go)

package neo
import (
	"encoding/binary"
)
`)

	// go over packet types declaration and generate marshal code for them
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


// info about wire decode/encode of a basic type
type basicCodec struct {
	wireSize int
	decode string
	encode string
}

var basicTypes = map[types.BasicKind]basicCodec {
	// decode: %v    will be `data[n:]`  (and made sure data has more enough bytes to read)
	// encode: %v %v will be `data[n:]`, value
	types.Bool:	{1, "byte2bool((%v)[0])", "(%v)[0] = bool2byte(%v)"},
	types.Int8:	{1, "int8((%v)[0])", "(%v)[0] = uint8(%v)" },
	types.Int16:	{2, "int16(binary.BigEndian.Uint16(%v))", "binary.BigEndian.PutUint16(uint16(%v))"},
	types.Int32:	{4, "int32(binary.BigEndian.Uint32(%v))", "binary.BigEndian.PutUint32(uint32(%v))"},
	types.Int64:	{8, "int64(binary.BigEndian.Uint64(%v))", "binary.BigEndian.PutUint64(uint64(%v))"},

	types.Uint8:	{1, "(%v)[0]", "(%v)[0] = %v"},
	types.Uint16:	{2, "binary.BigEndian.Uint16(%v)", "binary.BigEndian.PutUint16(%v, %v)"},
	types.Uint32:	{4, "binary.BigEndian.Uint32(%v)", "binary.BigEndian.PutUint32(%v, %v)"},
	types.Uint64:	{8, "binary.BigEndian.Uint64(%v)", "binary.BigEndian.PutUint64(%v, %v)"},

	types.Float64:	{8, "float64_NEODecode(%v)", "float64_NEOEncode(%v, %v)"},
}


// state of decode codegen
type decoder struct {
	buf bytes.Buffer	// buffer for generated code
	n int			// current decode position in data
}

func (d *decoder) emit(format string, a ...interface{}) {
	fmt.Fprintf(&d.buf, format+"\n", a...)
}

// emit code to decode basic fixed types (not string)
// userType is type actually used in source (for which typ is underlying), or nil
func (d *decoder) decodeBasic(assignto string, typ *types.Basic, userType types.Type, obj types.Object) {
	bdec, ok := basicTypes[typ.Kind()]
	if !ok {
		log.Fatalf("%v: %v: basic type %v not supported", pos(obj), obj.Name(), typ)
	}
	dataptr := fmt.Sprintf("data[%v:]", d.n)
	decoded := fmt.Sprintf(bdec.decode, dataptr)
	d.n += bdec.wireSize
	if userType != nil && userType != typ {
		// userType is a named type over some basic, like
		// type ClusterState int32
		// -> need to cast
		decoded = fmt.Sprintf("%v(%v)", typeName(userType), decoded)
	}
	// NOTE no space before "=" - to be able to merge with ":"
	// prefix and become defining assignment
	d.emit("%s= %s", assignto, decoded)
}

// emit code for decode next string or []byte
// TODO []byte support
func (d *decoder) decodeStrBytes(assignto string) {
	// len	u32
	// [len]byte
	d.emit("{")
	d.decodeBasic("l:", types.Typ[types.Uint32], nil, nil)
	d.emit("data = data[%v:]", d.n)
	d.emit("if uint32(len(data)) < l { return 0, ErrDecodeOverflow }")
	d.emit("%v = string(data[:l])", assignto)
	d.emit("data = data[l:]")
	d.emit("nread += %v + l", d.n)
	d.emit("}")
	d.n = 0
}

// TODO optimize for []byte
func (d *decoder) decodeSlice(assignto string, typ *types.Slice, obj types.Object) {
	// len	u32
	// [len]item
	d.emit("{")
	d.decodeBasic("l:", types.Typ[types.Uint32], nil, nil)
	d.emit("data = data[%v:]", d.n)
	d.emit("nread += %v", d.n)
	d.n = 0
	d.emit("%v = make(%v, l)", assignto, typeName(typ))
	// TODO size check
	// TODO if size(item)==const - check l in one go
	//d.emit("if len(data) < l { return 0, ErrDecodeOverflow }")
	d.emit("for i := 0; uint32(i) < l; i++ {")
	d.emit("a := &%s[i]", assignto)
	// XXX try to avoid (*) in a
	d.decodeType("(*a)", typ.Elem(), obj)
	d.emit("data = data[%v:]", d.n)	// FIXME wrt slice of slice ?
	d.emit("nread += %v", d.n)
	d.emit("}")
	//d.emit("%v = string(data[:l])", assignto)
	d.emit("}")
	d.n = 0
}

func (d *decoder) decodeMap(assignto string, typ *types.Map, obj types.Object) {
	// len  u32
	// [len](key, value)
	d.emit("{")
	d.decodeBasic("l:", types.Typ[types.Uint32], nil, nil)
	d.emit("data = data[%v:]", d.n)
	d.emit("nread += %v", d.n)
	d.n = 0
	d.emit("%v = make(%v, l)", assignto, typeName(typ))
	// TODO size check
	// TODO if size(item)==const - check l in one go
	//d.emit("if len(data) < l { return 0, ErrDecodeOverflow }")
	d.emit("m := %v", assignto)
	d.emit("for i := 0; uint32(i) < l; i++ {")
	d.decodeType("key:", typ.Key(), obj)

	switch typ.Elem().Underlying().(type) {
	// basic types can be directly assigned to map entry
	case *types.Basic:
		// XXX handle string
		d.decodeType("m[key]", typ.Elem(), obj)

	// otherwise assign via temporary
	default:
		d.emit("var v %v", typeName(typ.Elem()))
		d.decodeType("v", typ.Elem(), obj)
		d.emit("m[key] = v")
	}

	d.emit("data = data[%v:]", d.n)	// FIXME wrt map of map ?
	d.emit("nread += %v", d.n)
	d.emit("}")
	//d.emit("%v = string(data[:l])", assignto)
	d.emit("}")
	d.n = 0
}

// top-level driver for emitting decode code for type
// the code emitted is of kind:
//
//	<assignto> = decode(typ)
//
// obj is object that uses this type in source program (so in case of an error
// we can point to source location for where it happenned)

// TODO -> walkType and hook decoder/encoder via interface
func (d *decoder) decodeType(assignto string, typ types.Type, obj types.Object) {
	switch u := typ.Underlying().(type) {
	case *types.Basic:
		if u.Kind() == types.String {
			d.decodeStrBytes(assignto)
			break
		}

		d.decodeBasic(assignto, u, typ, obj)

	case *types.Array:
		// TODO optimize for [...]byte
		var i int64	// XXX because `u.Len() int64`
		for i = 0; i < u.Len(); i++ {
			d.decodeType(fmt.Sprintf("%v[%v]", assignto, i), u.Elem(), obj)
		}

	case *types.Struct:
		for i := 0; i < u.NumFields(); i++ {
			v := u.Field(i)
			d.decodeType(assignto + "." + v.Name(), v.Type(), v)
		}

	case *types.Slice:
		d.decodeSlice(assignto, u, obj)

	case *types.Map:
		d.decodeMap(assignto, u, obj)



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
	d.emit("var nread uint32")

	//n := 0
	//t := typespec.Type.(*ast.StructType)	// must be

	// type & object which refers to this type
	typ := info.Types[typespec.Type].Type
	obj := info.Defs[typespec.Name]

	d.decodeType("p", typ, obj)

	d.emit("return int(nread) + %v, nil", d.n)
	d.emit("}")
	return d.buf.String()
}
