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
//
// NOTE we do no try to emit clean/very clever code - for cases where compiler
// can do a good job the work is delegated to it.

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
	"strings"
)

// parsed & typechecked input
var fset = token.NewFileSet()
var info = &types.Info{
	Types: make(map[ast.Expr]types.TypeAndValue),
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
	var fv []*ast.File
	for _, src := range []string{"proto.go", "neo.go"} {
		f, err := parser.ParseFile(fset, src, nil, 0)
		if err != nil {
			log.Fatalf("parse: %v", err)
		}
		fv = append(fv, f)
	}

	//ast.Print(fset, fv[0])
	//return

	conf := types.Config{Importer: importer.Default()}
	neoPkg, err := conf.Check("neo", fset, fv, info)
	if err != nil {
		log.Fatalf("typecheck: %v", err)
	}
	neoQualifier = types.RelativeTo(neoPkg)


	// prologue
	f := fv[0]	// proto.go comes first
	buf := bytes.Buffer{}
	buf.WriteString(`// DO NOT EDIT - AUTOGENERATED (by protogen.go)

package neo
import (
	"encoding/binary"
	"sort"
)
`)

	// go over packet types declaration and generate marshal code for them
	pktCode := 0
	for _, decl := range f.Decls {
		// we look for types (which can be only under GenDecl)
		gendecl, ok := decl.(*ast.GenDecl)
		if !ok || gendecl.Tok != token.TYPE {
			continue
		}

		//fmt.Println(gendecl)
		//ast.Print(fset, gendecl)
		//continue

		for _, spec := range gendecl.Specs {
			typespec := spec.(*ast.TypeSpec) // must be because tok = TYPE
			typename := typespec.Name.Name

			switch typespec.Type.(type) {
			default:
				// we are only interested in struct types
				continue

			case *ast.StructType:
				//fmt.Printf("\n%s:\n", typename)
				//fmt.Println(typespec)
				//ast.Print(fset, typespec)
				//continue

				fmt.Fprintf(&buf, "// %d. %s\n\n", pktCode, typename)

				buf.WriteString(generateCodecCode(typespec, &sizer{}))
				buf.WriteString(generateCodecCode(typespec, &encoder{}))
				buf.WriteString(generateCodecCode(typespec, &decoder{}))

				pktCode++
			}
		}
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


// info about encode/decode of a basic fixed-size type
type basicCodec struct {
	wireSize int
	encode string
	decode string
}

var basicTypes = map[types.BasicKind]basicCodec {
	// decode: %v    will be `data[n:]`  (and made sure data has more enough bytes to read)
	// encode: %v %v will be `data[n:]`, value
	types.Bool:	{1, "(%v)[0] = bool2byte(%v)", "byte2bool((%v)[0])"},
	types.Int8:	{1, "(%v)[0] = uint8(%v)", "int8((%v)[0])"},
	types.Int16:	{2, "binary.BigEndian.PutUint16(%v, uint16(%v))", "int16(binary.BigEndian.Uint16(%v))"},
	types.Int32:	{4, "binary.BigEndian.PutUint32(%v, uint32(%v))", "int32(binary.BigEndian.Uint32(%v))"},
	types.Int64:	{8, "binary.BigEndian.PutUint64(%v, uint64(%v))", "int64(binary.BigEndian.Uint64(%v))"},

	types.Uint8:	{1, "(%v)[0] = %v", "(%v)[0]"},
	types.Uint16:	{2, "binary.BigEndian.PutUint16(%v, %v)", "binary.BigEndian.Uint16(%v)"},
	types.Uint32:	{4, "binary.BigEndian.PutUint32(%v, %v)", "binary.BigEndian.Uint32(%v)"},
	types.Uint64:	{8, "binary.BigEndian.PutUint64(%v, %v)", "binary.BigEndian.Uint64(%v)"},

	types.Float64:	{8, "float64_NEOEncode(%v, %v)", "float64_NEODecode(%v)"},
}

// does a type have fixed wire size and, if yes, what it is?
func typeSizeFixed(typ types.Type) (wireSize int, ok bool) {
	switch u := typ.Underlying().(type) {
	case *types.Basic:
		basic, ok := basicTypes[u.Kind()]
		if ok {
			return basic.wireSize, ok
		}

	case *types.Struct:
		for i := 0; i < u.NumFields(); i++ {
			size, ok := typeSizeFixed(u.Field(i).Type())
			if !ok {
				goto notfixed
			}
			wireSize += size
		}

		return wireSize, true

	case *types.Array:
		elemSize, ok := typeSizeFixed(u.Elem())
		if ok {
			return int(u.Len()) * elemSize, ok
		}
	}

notfixed:
	// everything else is of not fixed wire size
	return 0, false
}


// Buffer + bell & whistles
type Buffer struct {
	bytes.Buffer
}

func (b *Buffer) emit(format string, a ...interface{}) {
	fmt.Fprintf(b, format+"\n", a...)
}

// interface of codegenerator for coder/decoder
type CodecCodeGen interface {
	genPrologue(recvName, typeName string)
	genEpilogue()

	// emit code to process basic fixed types (not string)
	// userType is type actually used in source (for which typ is underlying), or nil
	genBasic(path string, typ *types.Basic, userType types.Type)

	genSlice(path string, typ *types.Slice, obj types.Object)
	genMap(path string, typ *types.Map, obj types.Object)

	// XXX particular case of slice
	genStrBytes(path string)

	generatedCode() string
}

// sizer/encode/decode codegen
type sizer struct {
	Buffer	// XXX
	n int
	symLenv []string	// symbolic lengths to add to size
	varSizeUsed bool	// whether var size was used
}

type encoder struct {
	Buffer	// XXX
	n int
}

type decoder struct {
	Buffer	// buffer for generated code
	n int	// current decode position in data
}

var _ CodecCodeGen = (*sizer)(nil)
var _ CodecCodeGen = (*encoder)(nil)
var _ CodecCodeGen = (*decoder)(nil)

func (s *sizer) generatedCode() string {
	return s.String()	// XXX -> d.buf.String() ?
}

func (e *encoder) generatedCode() string {
	return e.String()	// XXX -> d.buf.String() ?
}

func (d *decoder) generatedCode() string {
	return d.String()	// XXX -> d.buf.String() ?
}

func (s *sizer) genPrologue(recvName, typeName string) {
	s.emit("func (%s *%s) NEOEncodedLen() int {", recvName, typeName)
	if s.varSizeUsed {
		s.emit("var size int")
	}
}

func (e *encoder) genPrologue(recvName, typeName string) {
	e.emit("func (%s *%s) NEOEncode(data []byte) {", recvName, typeName)
}

func (d *decoder) genPrologue(recvName, typeName string) {
	d.emit("func (%s *%s) NEODecode(data []byte) (int, error) {", recvName, typeName)
	d.emit("var nread uint32")
}

func (s *sizer) genEpilogue() {
	size := fmt.Sprintf("%v", s.n)
	if len(s.symLenv) > 0 {
		size += " + " + strings.Join(s.symLenv, " + ")
	}
	if s.varSizeUsed {
		size += " + size"
	}
	s.emit("return %v", size)
	s.emit("}\n")
}

func (e *encoder) genEpilogue() {
	e.emit("}\n")
}

func (d *decoder) genEpilogue() {
	d.emit("return int(nread) + %v, nil", d.n)
	d.emit("\noverflow:")
	d.emit("return 0, ErrDecodeOverflow")
	d.emit("goto overflow")	// TODO check if overflow used at all and remove
	d.emit("}\n")
}

func (s *sizer) genBasic(path string, typ *types.Basic, userType types.Type) {
	basic := basicTypes[typ.Kind()]
	s.n += basic.wireSize
}

func (e *encoder) genBasic(path string, typ *types.Basic, userType types.Type) {
	basic := basicTypes[typ.Kind()]
	dataptr := fmt.Sprintf("data[%v:]", e.n)
	if userType != nil && userType != typ {
		// userType is a named type over some basic, like
		// type ClusterState int32
		// -> need to cast
		path = fmt.Sprintf("%v(%v)", typeName(typ), path)
	}
	e.n += basic.wireSize
	e.emit(basic.encode, dataptr, path)
}

func (d *decoder) genBasic(assignto string, typ *types.Basic, userType types.Type) {
	basic := basicTypes[typ.Kind()]
	d.emit("if len(data) < %v { goto overflow }", d.n + basic.wireSize)
	dataptr := fmt.Sprintf("data[%v:]", d.n)
	decoded := fmt.Sprintf(basic.decode, dataptr)
	d.n += basic.wireSize
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

// emit code to encode/decode string or []byte
// len	u32
// [len]byte
// TODO []byte support
func (s *sizer) genStrBytes(path string) {
	s.n += 4
	s.symLenv = append(s.symLenv, fmt.Sprintf("len(%s)", path))
	//s.n = 0
}

func (e *encoder) genStrBytes(path string) {
	e.emit("{")
	e.emit("l := uint32(len(%s))", path)
	e.genBasic("l", types.Typ[types.Uint32], nil)
	e.emit("data = data[%v:]", e.n)
	e.emit("copy(data, %v)", path)
	e.emit("data = data[l:]")
	e.emit("}")
	e.n = 0
}

func (d *decoder) genStrBytes(assignto string) {
	d.emit("{")
	d.genBasic("l:", types.Typ[types.Uint32], nil)
	d.emit("data = data[%v:]", d.n)
	d.emit("if uint32(len(data)) < l { goto overflow }")
	d.emit("%v= string(data[:l])", assignto)
	d.emit("data = data[l:]")
	d.emit("nread += %v + l", d.n)
	d.emit("}")
	d.n = 0
}

// emit code to encode/decode slice
// len	u32
// [len]item
// TODO optimize for []byte
func (s *sizer) genSlice(path string, typ *types.Slice, obj types.Object) {
	// if size(item)==const - size update in one go
	elemSize, ok := typeSizeFixed(typ.Elem())
	if ok {
		s.n += 4
		s.symLenv = append(s.symLenv, fmt.Sprintf("len(%v) * %v", path, elemSize))
		return
	}

	s.varSizeUsed = true
	s.n += 4
	s.emit("size += %v", s.n)
	s.n = 0
	s.emit("for i := 0; i < len(%v); i++ {", path)
	s.emit("a := &%s[i]", path)
	//codegenType("(*a)", typ.Elem(), obj, s)
	codegenType("(*a)", typ.Elem(), obj, &sizer{})
	s.emit("size += %v", s.n)
	s.emit("}")
	s.n = 0
}

func (e *encoder) genSlice(path string, typ *types.Slice, obj types.Object) {
	e.emit("{")
	e.emit("l := uint32(len(%s))", path)
	e.genBasic("l", types.Typ[types.Uint32], nil)
	e.emit("data = data[%v:]", e.n)
	e.n = 0
	e.emit("for i := 0; uint32(i) <l; i++ {")
	e.emit("a := &%s[i]", path)
	codegenType("(*a)", typ.Elem(), obj, e)
	e.emit("data = data[%v:]", e.n)	// FIXME wrt slice of slice ?
	e.emit("}")
	// see vvv
	e.emit("}")
	e.n = 0
}

func (d *decoder) genSlice(assignto string, typ *types.Slice, obj types.Object) {
	d.emit("{")
	d.genBasic("l:", types.Typ[types.Uint32], nil)
	d.emit("data = data[%v:]", d.n)
	d.emit("nread += %v", d.n)
	d.n = 0
	d.emit("%v= make(%v, l)", assignto, typeName(typ))
	// TODO size check
	// TODO if size(item)==const - check l in one go
	//d.emit("if len(data) < l { goto overflow }")
	d.emit("for i := 0; uint32(i) < l; i++ {")
	d.emit("a := &%s[i]", assignto)
	// XXX try to avoid (*) in a
	codegenType("(*a)", typ.Elem(), obj, d)
	d.emit("data = data[%v:]", d.n)	// FIXME wrt slice of slice ?
	d.emit("nread += %v", d.n)
	d.emit("}")
	//d.emit("%v= string(data[:l])", assignto)
	d.emit("}")
	d.n = 0
}

// generate code to encode/decode map
// len  u32
// [len](key, value)
func (s *sizer) genMap(path string, typ *types.Map, obj types.Object) {
	keySize, keyFixed := typeSizeFixed(typ.Key())
	elemSize, elemFixed := typeSizeFixed(typ.Elem())

	if keyFixed && elemFixed {
		s.emit("size += 4 + len(%v) * %v", path, keySize + elemSize)
		return
	}

	s.emit("size += %v + 4", s.n)
	s.n = 0
	s.emit("for key := range %s {", path)
	codegenType("key", typ.Key(), obj, s)
	codegenType(fmt.Sprintf("%s[key]", path), typ.Elem(), obj, s)
	s.emit("size += %v", s.n)
	s.emit("}")
	s.n = 0
}

func (e *encoder) genMap(path string, typ *types.Map, obj types.Object) {
	e.emit("{")
	e.emit("l := uint32(len(%s))", path)
	e.genBasic("l", types.Typ[types.Uint32], nil)
	e.emit("data = data[%v:]", e.n)
	e.n = 0
	// output keys in sorted order on the wire
	// (easier for debugging & deterministic for testing)
	e.emit("keyv := make([]%s, 0, l)", typeName(typ.Key()))
	e.emit("for key := range %s {", path)
	e.emit("  keyv = append(keyv, key)")
	e.emit("}")
	e.emit("sort.Slice(keyv, func (i, j int) bool { return keyv[i] < keyv[j] })")
	e.emit("for _, key := range keyv {")
	codegenType("key", typ.Key(), obj, e)
	codegenType(fmt.Sprintf("%s[key]", path), typ.Elem(), obj, e)
	e.emit("data = data[%v:]", e.n)	// XXX wrt map of map?
	e.emit("}")
	// XXX vvv ?
	e.emit("}")
	e.n = 0
}

func (d *decoder) genMap(assignto string, typ *types.Map, obj types.Object) {
	d.emit("{")
	d.genBasic("l:", types.Typ[types.Uint32], nil)
	d.emit("data = data[%v:]", d.n)
	d.emit("nread += %v", d.n)
	d.n = 0
	d.emit("%v= make(%v, l)", assignto, typeName(typ))
	// TODO size check
	// TODO if size(item)==const - check l in one go
	//d.emit("if len(data) < l { goto overflow }")
	d.emit("m := %v", assignto)
	d.emit("for i := 0; uint32(i) < l; i++ {")
	codegenType("key:", typ.Key(), obj, d)

	switch typ.Elem().Underlying().(type) {
	// basic types can be directly assigned to map entry
	case *types.Basic:
		// XXX handle string
		codegenType("m[key]", typ.Elem(), obj, d)

	// otherwise assign via temporary
	default:
		d.emit("var v %v", typeName(typ.Elem()))
		codegenType("v", typ.Elem(), obj, d)
		d.emit("m[key] = v")
	}

	d.emit("data = data[%v:]", d.n)	// FIXME wrt map of map ?
	d.emit("nread += %v", d.n)
	d.emit("}")
	//d.emit("%v= string(data[:l])", assignto)
	d.emit("}")
	d.n = 0
}

// top-level driver for emitting encode/decode code for a type
// the code emitted is of kind:
//
//	<assignto> = decode(typ)
//
// obj is object that uses this type in source program (so in case of an error
// we can point to source location for where it happenned)

func codegenType(path string, typ types.Type, obj types.Object, codegen CodecCodeGen) {
	switch u := typ.Underlying().(type) {
	case *types.Basic:
		if u.Kind() == types.String {
			codegen.genStrBytes(path)
			break
		}

		_, ok := basicTypes[u.Kind()]
		if !ok {
			log.Fatalf("%v: %v: basic type %v not supported", pos(obj), obj.Name(), u)
		}
		codegen.genBasic(path, u, typ)

	case *types.Struct:
		for i := 0; i < u.NumFields(); i++ {
			v := u.Field(i)
			codegenType(path + "." + v.Name(), v.Type(), v, codegen)
		}

	case *types.Array:
		// TODO optimize for [...]byte
		var i int64	// XXX because `u.Len() int64`
		for i = 0; i < u.Len(); i++ {
			codegenType(fmt.Sprintf("%v[%v]", path, i), u.Elem(), obj, codegen)
		}

	case *types.Slice:
		codegen.genSlice(path, u, obj)

	case *types.Map:
		codegen.genMap(path, u, obj)



	default:
		log.Fatalf("%v: %v has unsupported type %v (%v)", pos(obj),
			obj.Name(), typ, u)
	}
}


// generate encoder/decode funcs for a type declaration typespec
func generateCodecCode(typespec *ast.TypeSpec, codec CodecCodeGen) string {
	codec.genPrologue("p", typespec.Name.Name)

	// type & object which refers to this type
	typ := info.Types[typespec.Type].Type
	obj := info.Defs[typespec.Name]

	codegenType("p", typ, obj, codec)

	codec.genEpilogue()
	return codec.generatedCode()
}
