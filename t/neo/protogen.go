// Copyright (C) 2016-2017  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 2, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

// +build ignore

/*
NEO. Protocol module. Code generator

This program generates marshalling code for packet types defined in proto.go .
For every type 3 methods are generated in accordance with NEOEncoder and
NEODecoder interfaces:

	NEOEncodedLen() int
	NEOEncode(buf []byte)
	NEODecode(data []byte) (nread int, err error)

List of packet types is obtained via searching through proto.go AST - looking
for appropriate struct declarations there.

Code generation for a type is organized via recursively walking through type's
(sub-)elements and generating specialized code on leaf items (intX, slices,
maps, ...).

Top-level generation driver is in generateCodecCode(). It accepts type
specification and something that performs actual leaf-nodes code generation
(CodeGenerator interface). There are 3 particular codegenerators implemented -
- sizeCodeGen, encoder & decoder - to generate each of the needed method functions. XXX naming

The structure of whole process is very similar to what would be happening at
runtime if marshalling was reflect based, but statically with go/types we don't
spend runtime time on decisions and thus generated marshallers are faster.

For encoding format compatibility with Python NEO (neo/lib/protocol.py) is
preserved in order for two implementations to be able to communicate to each
other.

NOTE we do no try to emit very clever code - for cases where compiler
can do a good job the work is delegated to it.
*/

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

	// go through proto.go and AST'ify & typecheck it
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
				fmt.Fprintf(&buf, "// %d. %s\n\n", pktCode, typename)

				buf.WriteString(generateCodecCode(typespec, &sizeCodeGen{}))
				buf.WriteString(generateCodecCode(typespec, &encoder{}))
				buf.WriteString(generateCodecCode(typespec, &decoder{}))

				pktCode++
			}
		}
	}

	// format & emit generated code
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
	// encode: %v %v will be `data[n:]`, value
	// decode: %v    will be `data[n:]`  (and already made sure data has more enough bytes to read)
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

// does a type have fixed wire size == 1 ?
func typeSizeFixed1(typ types.Type) bool {
	wireSize, _ := typeSizeFixed(typ)
	return wireSize == 1
}


// bytes.Buffer + bell & whistles
type Buffer struct {
	bytes.Buffer
}

func (b *Buffer) emit(format string, a ...interface{}) {
	fmt.Fprintf(b, format+"\n", a...)
}

// interface of a codegenerator  (for sizeCodeGen/coder/decoder XXX naming)
type CodeGenerator interface {
	// tell codegen it should generate code for which type & receiver name
	setFunc(recvName, typeName string, typ types.Type)

	// generate code to process a basic fixed type (not string)
	// userType is type actually used in source (for which typ is underlying), or nil
	// path is associated data member (to read from or write to)
	genBasic(path string, typ *types.Basic, userType types.Type)

	// generate code to process slice or map
	// (see genBasic for argument details)
	genSlice(path string, typ *types.Slice, obj types.Object)
	genMap(path string, typ *types.Map, obj types.Object)

	// particular case of slice or array with 1-byte elem
	//
	// NOTE this particular case is kept separate because for 1-byte
	// elements there are no byteordering issues so data can be directly
	// either accessed or copied.
	genSlice1(path string, typ types.Type)
	genArray1(path string, typ *types.Array)

	// get generated code.
	generatedCode() string
}

// common part of codegenerators
type commonCodeGen struct {
	buf      Buffer		// code is emitted here

	recvName string		// receiver/type for top-level func
	typeName string		// or empty
	typ      types.Type

	varUsed map[string]bool	// whether a variable was used
}

func (c *commonCodeGen) emit(format string, a ...interface{}) {
	c.buf.emit(format, a...)
}

func (c *commonCodeGen) setFunc(recvName, typeName string, typ types.Type) {
	c.recvName = recvName
	c.typeName = typeName
	c.typ = typ
}

// get variable for varname  (and automatically mark this var as used)
func (c *commonCodeGen) var_(varname string) string {
	if c.varUsed == nil {
		c.varUsed = make(map[string]bool)
	}
	c.varUsed[varname] = true
	return varname
}

// symbolic size
// consists of numeric & symbolic expression parts
// size is num + expr1 + expr2 + ...
type SymSize struct {
	num	int	 // numeric part of size
	exprv	[]string // symbolic part of size
}

func (s *SymSize) Add(n int) {
	s.num += n
}

func (s *SymSize) AddExpr(format string, a ...interface{}) {
	expr := fmt.Sprintf(format, a...)
	s.exprv = append(s.exprv, expr)
}

func (s *SymSize) String() string {
	// num + expr1 + expr2 + ...  (omitting what is possible)
	sizev := []string{}
	if s.num != 0 {
		sizev = append(sizev, fmt.Sprintf("%v", s.num))
	}
	exprStr := s.ExprString()
	if exprStr != "" {
		sizev = append(sizev, exprStr)
	}
	sizeStr := strings.Join(sizev, " + ")
	if sizeStr == "" {
		sizeStr = "0"
	}
	return sizeStr
}

// expression part of size  (without numeric part)
func (s *SymSize) ExprString() string {
	return strings.Join(s.exprv, " + ")
}

func (s *SymSize) IsZero() bool {
	return s.num == 0 && len(s.exprv) == 0
}

func (s *SymSize) Reset() {
	*s = SymSize{}
}


// decoder overflow check state
type OverflowCheck struct {
	// size to check for overflow
	size    SymSize

	// whether overflow was already checked for current decodings
	// (if yes, size updates will be ignored)
	checked bool

	// stack operated by {Push,Pop}Checked
	checkedStk []bool
}

// push/pop checked state
func (o *OverflowCheck) PushChecked(checked bool) {
	o.checkedStk = append(o.checkedStk, o.checked)
	o.checked = checked
}

func (o *OverflowCheck) PopChecked() bool {
	popret := o.checked
	l := len(o.checkedStk)
	o.checked = o.checkedStk[l-1]
	o.checkedStk = o.checkedStk[:l-1]
	return popret
}



// Add and AddExpr update .size accordingly, but only if overflow was not
// already marked as checked
func (o *OverflowCheck) Add(n int) {
	if !o.checked {
		o.size.Add(n)
	}
}

func (o *OverflowCheck) AddExpr(format string, a ...interface{}) {
	if !o.checked {
		o.size.AddExpr(format, a...)
	}
}


// XXX naming ok?
// XXX -> Gen_NEOEncodedLen ?
// sizeCodeGen generates code to compute encoded size of a packet
//
// when type is recursively walked for every case symbolic size is added appropriately
// in case when it was needed to generate loops runtime accumulator variable is additionally used
// result is: symbolic size + (optionally) runtime accumulator
type sizeCodeGen struct {
	commonCodeGen
	size   SymSize	// currently accumulated packet size
}

// encoder generates code to encode a packet
//
// when type is recursively walked for every case code to update `data[n:]` is generated.
// no overflow checks are generated as by NEOEncoder interface provided data
// buffer should have at least NEOEncodedLen() length (the size computed by
// sizeCodeGen)
type encoder struct {
	commonCodeGen
	n int	// current write position in data
}

// decoder generates code to decode a packet
//
// when type is recursively walked for every case code to decode next item from
// `data[n:]` is generated.
//
// overflow checks and, when convenient, nread updates are grouped and emitted
// so that they are performed in the beginning of greedy fixed-wire-size
// blocks - checking as much as possible in one go.
//
// TODO more text?
type decoder struct {
	commonCodeGen

	// done buffer for generated code
	// current delayed overflow check will be inserted in between bufDone & buf
	bufDone Buffer

	n int	// current read position in data.

	// size that will be checked for overflow at current overflow check point
	overflowCheck OverflowCheck
}

var _ CodeGenerator = (*sizeCodeGen)(nil)
var _ CodeGenerator = (*encoder)(nil)
var _ CodeGenerator = (*decoder)(nil)


func (s *sizeCodeGen) generatedCode() string {
	code := Buffer{}
	// prologue
	code.emit("func (%s *%s) NEOEncodedLen() int {", s.recvName, s.typeName)
	if s.varUsed["size"] {
		code.emit("var %s int", s.var_("size"))
	}

	code.Write(s.buf.Bytes())

	// epilogue
	size := s.size.String()
	if s.varUsed["size"] {
		size += " + " + s.var_("size")
	}
	code.emit("return %v", size)
	code.emit("}\n")

	return code.String()
}

func (e *encoder) generatedCode() string {
	code := Buffer{}
	// prologue
	code.emit("func (%s *%s) NEOEncode(data []byte) {", e.recvName, e.typeName)

	code.Write(e.buf.Bytes())

	// epilogue
	code.emit("}\n")

	return code.String()
}

// XXX place?
// data <- data[pos:]
// pos  <- 0
func (d *decoder) resetPos() {
	if d.n != 0 {
		d.emit("data = data[%v:]", d.n)
		d.emit("%v += %v", d.var_("nread"), d.n)
	}

	d.n = 0
}

// XXX place?
// mark current place for delayed insertion of overflow check code
//
// delayed: because we go forward in decode path scanning ahead as far as we
// can - until first variable-size encoded something, and then - knowing fixed
// size would be read - insert checking condition for accumulated size to
// here-marked overflow checkpoint.
//
// so overflowCheckpoint does:
// 1. emit overflow checking code for previous overflow checkpoint
// 2. mark current place as next overflow checkpoint to eventually emit
//
// it is inserted
// - before reading a variable sized item
// - in the beginning of a loop inside
func (d *decoder) overflowCheckpoint() {
	//d.bufDone.emit("// overflow check point")
	if !d.overflowCheck.size.IsZero() {
		d.bufDone.emit("if uint32(len(data)) < %v { goto overflow }", &d.overflowCheck.size)
	}

	d.overflowCheck.size.Reset()

	d.bufDone.Write(d.buf.Bytes())
	d.buf.Reset()
}

func (d *decoder) generatedCode() string {
	d.overflowCheckpoint()

	code := Buffer{}
	// prologue
	code.emit("func (%s *%s) NEODecode(data []byte) (int, error) {", d.recvName, d.typeName)
	if d.varUsed["nread"] {
		code.emit("var %v uint32", d.var_("nread"))
	}

	code.Write(d.bufDone.Bytes())

	// epilogue
	retexpr := fmt.Sprintf("%v", d.n)
	if d.varUsed["nread"] {
		retexpr += fmt.Sprintf(" + int(%v)", d.var_("nread"))
	}
	code.emit("return %v, nil", retexpr)

	// `goto overflow` is not used only for empty structs
	if (&types.StdSizes{8, 8}).Sizeof(d.typ) > 0 {
		code.emit("\noverflow:")
		code.emit("return 0, ErrDecodeOverflow")
	}
	code.emit("}\n")

	return code.String()
}

// emit code to size/encode/decode basic fixed type
func (s *sizeCodeGen) genBasic(path string, typ *types.Basic, userType types.Type) {
	basic := basicTypes[typ.Kind()]
	s.size.Add(basic.wireSize)
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
	dataptr := fmt.Sprintf("data[%v:]", d.n)
	decoded := fmt.Sprintf(basic.decode, dataptr)
	d.n += basic.wireSize
	d.overflowCheck.Add(basic.wireSize)
	if userType != nil && userType != typ {
		// need to cast (like in encode case)
		decoded = fmt.Sprintf("%v(%v)", typeName(userType), decoded)
	}
	// NOTE no space before "=" - to be able to merge with ":"
	// prefix and become defining assignment
	d.emit("%s= %s", assignto, decoded)
}

// emit code to size/encode/decode string or []byte
// len	u32
// [len]byte
func (s *sizeCodeGen) genSlice1(path string, typ types.Type) {
	s.size.Add(4)
	s.size.AddExpr("len(%s)", path)
}

func (e *encoder) genSlice1(path string, typ types.Type) {
	e.emit("{")
	e.emit("l := uint32(len(%s))", path)
	e.genBasic("l", types.Typ[types.Uint32], nil)
	e.emit("data = data[%v:]", e.n)
	e.emit("copy(data, %v)", path)
	e.emit("data = data[l:]")
	e.emit("}")
	e.n = 0
}

func (d *decoder) genSlice1(assignto string, typ types.Type) {
	d.emit("{")
	d.genBasic("l:", types.Typ[types.Uint32], nil)

	d.emit("data = data[%v:]", d.n)
	d.emit("%v += %v + l", d.var_("nread"), d.n)
	d.n = 0

	d.overflowCheckpoint()
	d.overflowCheck.AddExpr("l")

	switch t := typ.(type) {
	case *types.Basic:
		if t.Kind() != types.String {
			log.Panicf("bad basic type in slice1: %v", t)
		}
		d.emit("%v= string(data[:l])", assignto)

	case *types.Slice:
		// TODO eventually do not copy, but reference data from original
		d.emit("%v= make(%v, l)", assignto, typeName(typ))
		d.emit("copy(%v, data[:l])", assignto)

	default:
		log.Panicf("bad type in slice1: %v", typ)
	}

	d.emit("data = data[l:]")
	d.emit("}")
}

// emit code to size/encode/decode array with sizeof(elem)==1
// [len(A)]byte
func (s *sizeCodeGen) genArray1(path string, typ *types.Array) {
	s.size.Add(int(typ.Len()))
}

func (e *encoder) genArray1(path string, typ *types.Array) {
	e.emit("copy(data[%v:], %v[:])", e.n, path)
	e.n += int(typ.Len())
}

func (d *decoder) genArray1(assignto string, typ *types.Array) {
	typLen := int(typ.Len())
	d.emit("copy(%v[:], data[%v:%v])", assignto, d.n, d.n + typLen)
	d.n += typLen
	d.overflowCheck.Add(typLen)
}


// emit code to size/encode/decode slice
// len	u32
// [len]item
func (s *sizeCodeGen) genSlice(path string, typ *types.Slice, obj types.Object) {
	s.size.Add(4)

	// if size(item)==const - size update in one go
	elemSize, ok := typeSizeFixed(typ.Elem())
	if ok {
		s.size.AddExpr("len(%v) * %v", path, elemSize)
		return
	}

	curSize := s.size
	s.size.Reset()

	s.emit("for i := 0; i < len(%v); i++ {", path)
	s.emit("a := &%s[i]", path)

	codegenType("(*a)", typ.Elem(), obj, s)

	// merge-in size updates
	s.emit("%v += %v", s.var_("size"), s.size.ExprString())
	s.emit("}")
	if s.size.num != 0 {
		curSize.AddExpr("len(%v) * %v", path, s.size.num)
	}
	s.size = curSize
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
	e.emit("}")
	e.n = 0
}

func (d *decoder) genSlice(assignto string, typ *types.Slice, obj types.Object) {
	d.emit("{")
	d.genBasic("l:", types.Typ[types.Uint32], nil)

	d.resetPos()

	// if size(item)==const - check overflow in one go
	elemSize, elemFixed := typeSizeFixed(typ.Elem())
	if elemFixed {
		d.overflowCheckpoint()
		d.overflowCheck.AddExpr("l * %v", elemSize)
		d.overflowCheck.PushChecked(true)
		defer d.overflowCheck.PopChecked()

		d.emit("%v += l * %v", d.var_("nread"), elemSize)
	}


	d.emit("%v= make(%v, l)", assignto, typeName(typ))
	d.emit("for i := 0; uint32(i) < l; i++ {")
	d.emit("a := &%s[i]", assignto)

	if !elemFixed {
		d.overflowCheckpoint()
	}
	codegenType("(*a)", typ.Elem(), obj, d)

	// d.resetPos() with nread update optionally skipped
	if d.n != 0 {
		d.emit("data = data[%v:]", d.n)
		if !elemFixed {
			d.emit("%v += %v", d.var_("nread"), d.n)
		}
		d.n = 0
	}

	d.emit("}")
	d.emit("}")
}

// generate code to encode/decode map
// len  u32
// [len](key, value)
func (s *sizeCodeGen) genMap(path string, typ *types.Map, obj types.Object) {
	keySize, keyFixed := typeSizeFixed(typ.Key())
	elemSize, elemFixed := typeSizeFixed(typ.Elem())

	if keyFixed && elemFixed {
		s.size.Add(4)
		s.size.AddExpr("len(%v) * %v", path, keySize + elemSize)
		return
	}

	s.size.Add(4)
	curSize := s.size
	s.size.Reset()

	// FIXME for map of map gives ...[key][key] => key -> different variables
	s.emit("for key := range %s {", path)
	codegenType("key", typ.Key(), obj, s)
	codegenType(fmt.Sprintf("%s[key]", path), typ.Elem(), obj, s)

	// merge-in size updates
	s.emit("%v += %v", s.var_("size"), s.size.ExprString())
	s.emit("}")
	if s.size.num != 0 {
		curSize.AddExpr("len(%v) * %v", path, s.size.num)
	}
	s.size = curSize
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
	e.emit("}")
	e.n = 0
}

func (d *decoder) genMap(assignto string, typ *types.Map, obj types.Object) {
	d.emit("{")
	d.genBasic("l:", types.Typ[types.Uint32], nil)

	d.resetPos()

	// if size(key,item)==const - check overflow in one go
	keySize, keyFixed := typeSizeFixed(typ.Key())
	elemSize, elemFixed := typeSizeFixed(typ.Elem())
	itemFixed := keyFixed && elemFixed
	if itemFixed {
		d.overflowCheckpoint()
		d.overflowCheck.AddExpr("l * %v", keySize + elemSize)
		d.overflowCheck.PushChecked(true)
		defer d.overflowCheck.PopChecked()

		d.emit("%v += l * %v", d.var_("nread"), keySize + elemSize)
	}

	d.emit("%v= make(%v, l)", assignto, typeName(typ))
	d.emit("m := %v", assignto)
	d.emit("for i := 0; uint32(i) < l; i++ {")

	if !itemFixed {
		d.overflowCheckpoint()
	}

	codegenType("key:", typ.Key(), obj, d)

	switch typ.Elem().Underlying().(type) {
	// basic types can be directly assigned to map entry
	case *types.Basic:
		codegenType("m[key]", typ.Elem(), obj, d)

	// otherwise assign via temporary
	default:
		d.emit("var v %v", typeName(typ.Elem()))
		codegenType("v", typ.Elem(), obj, d)
		d.emit("m[key] = v")
	}

	// d.resetPos() with nread update optionally skipped
	if d.n != 0 {
		d.emit("data = data[%v:]", d.n)
		if !itemFixed {
			d.emit("%v += %v", d.var_("nread"), d.n)
		}
		d.n = 0
	}

	d.emit("}")
	d.emit("}")
}

// top-level driver for emitting encode/decode code for a type
// the code emitted is of kind:
//
//	<assignto> = decode(typ)
//
// obj is object that uses this type in source program (so in case of an error
// we can point to source location for where it happenned)

func codegenType(path string, typ types.Type, obj types.Object, codegen CodeGenerator) {
	switch u := typ.Underlying().(type) {
	case *types.Basic:
		if u.Kind() == types.String {
			codegen.genSlice1(path, u)
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
		// [...]byte or [...]uint8 - just straight copy
		if typeSizeFixed1(u.Elem()) {
			codegen.genArray1(path, u)
		} else {
			var i int64	// XXX because `u.Len() int64`
			for i = 0; i < u.Len(); i++ {
				codegenType(fmt.Sprintf("%v[%v]", path, i), u.Elem(), obj, codegen)
			}
		}

	case *types.Slice:
		if typeSizeFixed1(u.Elem()) {
			codegen.genSlice1(path, u)
		} else {
			codegen.genSlice(path, u, obj)
		}

	case *types.Map:
		codegen.genMap(path, u, obj)



	default:
		log.Fatalf("%v: %v has unsupported type %v (%v)", pos(obj),
			obj.Name(), typ, u)
	}
}


// generate encoder/decode funcs for a type declaration typespec
// XXX name? -> genMethCode ?  generateMethodCode ?
func generateCodecCode(typespec *ast.TypeSpec, codegen CodeGenerator) string {
	// type & object which refers to this type
	typ := info.Types[typespec.Type].Type
	obj := info.Defs[typespec.Name]

	codegen.setFunc("p", typespec.Name.Name, typ)
	codegenType("p", typ, obj, codegen)

	return codegen.generatedCode()
}
