// Copyright (C) 2016-2018  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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

// +build ignore

/*
NEO. Protocol module. Code generator

This program generates marshalling code for message types defined in proto.go .
For every type 4 methods are generated in accordance with neo.Msg interface:

	NEOMsgCode() uint16
	NEOMsgEncodedLen() int
	NEOMsgEncode(buf []byte)
	NEOMsgDecode(data []byte) (nread int, err error)

List of message types is obtained via searching through proto.go AST - looking
for appropriate struct declarations there.

Code generation for a type is organized via recursively walking through type's
(sub-)elements and generating specialized code on leaf items (intX, slices,
maps, ...).

Top-level generation driver is in generateCodecCode(). It accepts type
specification and something that performs actual leaf-nodes code generation
(CodeGenerator interface). There are 3 particular codegenerators implemented -
- sizer, encoder & decoder - to generate each of the needed method functions.

The structure of whole process is very similar to what would be happening at
runtime if marshalling was reflect based, but statically with go/types we don't
spend runtime time on decisions and thus generated marshallers are faster.

For encoding format compatibility with Python NEO (neo/lib/protocol.py) is
preserved in order for two implementations to be able to communicate to each
other.

NOTE we do no try to emit very clever code - for cases where compiler
can do a good job the work is delegated to it.

--------

Also along the way types registry table is generated for

	msgCode -> message type

lookup needed in packet receive codepath.
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
	"sort"
	"strings"
)

// parsed & typechecked input
var fset = token.NewFileSet()
var fileMap = map[string]*ast.File{}      // fileName -> AST
var pkgMap  = map[string]*types.Package{} // pkgPath  -> Package
var typeInfo = &types.Info{
	Types: make(map[ast.Expr]types.TypeAndValue),
	Defs:  make(map[*ast.Ident]types.Object),
}

// complete position of something with .Pos()
func pos(x interface{ Pos() token.Pos }) token.Position {
	return fset.Position(x.Pos())
}

// get type name in context of neo package
var zodbPkg  *types.Package
var protoPkg *types.Package
func typeName(typ types.Type) string {
	qf := func(pkg *types.Package) string {
		switch pkg {
		case protoPkg:
			// same package - unqualified
			return ""

		case zodbPkg:
			// zodb is imported - only name
			return pkg.Name()

		default:
			// fully qualified otherwise
			return pkg.Path()
		}
	}

	return types.TypeString(typ, qf)
}

var neo_customCodec *types.Interface // type of neo.customCodec
var memBuf          types.Type       // type of mem.Buf

// bytes.Buffer + bell & whistles
type Buffer struct {
	bytes.Buffer
}

func (b *Buffer) emit(format string, a ...interface{}) {
	fmt.Fprintf(b, format+"\n", a...)
}


// importer that takes into account our already-loaded packages
type localImporter struct {
	types.Importer
}

func (li *localImporter) Import(path string) (*types.Package, error) {
	pkg := pkgMap[path]
	if pkg != nil {
		return pkg, nil
	}

	return li.Importer.Import(path)
}

// importer instance - only 1 so that for 2 top-level packages same dependent
// packages are not reimported several times.
var localImporterObj = &localImporter{importer.Default()}

func loadPkg(pkgPath string, sources ...string) *types.Package {
	var filev []*ast.File

	// parse
	for _, src := range sources {
		f, err := parser.ParseFile(fset, src, nil, parser.ParseComments)
		if err != nil {
			log.Fatalf("parse: %v", err)
		}
		fileMap[src] = f
		filev = append(filev, f)
	}

	//ast.Print(fset, fv[0])
	//return

	// typecheck
	conf := types.Config{Importer: localImporterObj}
	pkg, err := conf.Check(pkgPath, fset, filev, typeInfo)
	if err != nil {
		log.Fatalf("typecheck: %v", err)
	}
	pkgMap[pkgPath] = pkg
	return pkg
}

// `//neo:proto ...` annotations
type Annotation struct {
	typeonly bool
	answer   bool
}

// parse checks doc for specific comment annotations and, if present, loads them.
func (a *Annotation) parse(doc *ast.CommentGroup) {
	if doc == nil {
		return // for many types .Doc = nil if there is no comments
	}

	for _, comment := range doc.List {
		cpos := pos(comment)
		if !(cpos.Column == 1 && strings.HasPrefix(comment.Text, "//neo:proto ")) {
			continue
		}

		__ := strings.SplitN(comment.Text, " ", 2)
		arg := __[1]

		switch arg {
		case "typeonly":
			if a.typeonly {
				log.Fatalf("%v: duplicate `typeonly`", cpos)
			}
			a.typeonly = true

		case "answer":
			if a.answer {
				log.Fatalf("%v: duplicate `answer`", cpos)
			}
			a.answer = true

		default:
			log.Fatalf("%v: unknown neo:proto directive %q", cpos, arg)
		}
	}
}

// MsgCode represents message code in symbolic form: `serial (| answerBit)?`
type MsgCode struct {
	msgSerial int
	answer    bool
}

func (c MsgCode) String() string {
	s := fmt.Sprintf("%d", c.msgSerial)
	if c.answer {
		s += " | answerBit"
	}
	return s
}

// sort MsgCode by (serial, answer)
type BySerial []MsgCode

func (v BySerial) Less(i, j int) bool {
	return (v[i].msgSerial < v[j].msgSerial) ||
		(v[i].msgSerial == v[j].msgSerial && !v[i].answer && v[j].answer)
}
func (v BySerial) Swap(i, j int) { v[i], v[j] = v[j], v[i] }
func (v BySerial) Len() int      { return len(v) }

// ----------------------------------------

func main() {
	var err error

	log.SetFlags(0)

	// go through proto.go and AST'ify & typecheck it
	zodbPkg  = loadPkg("lab.nexedi.com/kirr/neo/go/zodb", "../../zodb/zodb.go")
	protoPkg = loadPkg("lab.nexedi.com/kirr/neo/go/neo/proto", "proto.go")

	// extract neo.customCodec
	cc := protoPkg.Scope().Lookup("customCodec")
	if cc == nil {
		log.Fatal("cannot find `customCodec`")
	}
	var ok bool
	neo_customCodec, ok = cc.Type().Underlying().(*types.Interface)
	if !ok {
		log.Fatal("customCodec is not interface  (got %v)", cc.Type())
	}

	// extract mem.Buf
	memPath := "lab.nexedi.com/kirr/go123/mem"
	var memPkg *types.Package
	for _, pkg := range zodbPkg.Imports() {
		if pkg.Path() == memPath {
			memPkg = pkg
			break
		}
	}
	if memPkg == nil {
		log.Fatalf("cannot find `%s` in zodb imports", memPath)
	}

	__ := memPkg.Scope().Lookup("Buf")
	if __ == nil {
		log.Fatal("cannot find `mem.Buf`")
	}
	memBuf = __.Type()

	// prologue
	f := fileMap["proto.go"]
	buf := Buffer{}
	buf.emit(`// Code generated by protogen.go; DO NOT EDIT.

package proto
// NEO. protocol messages to/from wire marshalling.

import (
	"encoding/binary"
	"reflect"
	"sort"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/neo/go/zodb"
)`)

	msgTypeRegistry := map[MsgCode]string{} // msgCode -> typename

	// go over message types declaration and generate marshal code for them
	buf.emit("// messages marshalling\n")
	msgSerial := 0
	for _, decl := range f.Decls {
		// we look for types (which can be only under GenDecl)
		gendecl, ok := decl.(*ast.GenDecl)
		if !ok || gendecl.Tok != token.TYPE {
			continue
		}

		//fmt.Println(gendecl)
		//ast.Print(fset, gendecl)
		//continue

		// `//neo:proto ...` annotations for whole decl
		// (e.g. <here> type ( t1 struct{...}; t2 struct{...} )
		declAnnotation := Annotation{}
		declAnnotation.parse(gendecl.Doc)

		for _, spec := range gendecl.Specs {
			typespec := spec.(*ast.TypeSpec) // must be because tok = TYPE
			typename := typespec.Name.Name

			// we are only interested in struct types
			if _, ok := typespec.Type.(*ast.StructType); !ok {
				continue
			}

			// `//neo:proto ...` annotation for this particular type
			specAnnotation := declAnnotation // inheriting from decl
			specAnnotation.parse(typespec.Doc)

			// type only -> don't generate message interface for it
			if specAnnotation.typeonly {
				continue
			}

			// generate code for this type to implement neo.Msg
			var msgCode MsgCode
			msgCode.answer = specAnnotation.answer || strings.HasPrefix(typename, "Answer")
			switch {
			case !msgCode.answer || typename == "Error":
				msgCode.msgSerial = msgSerial

			// answer to something
			default:
				msgCode.msgSerial = msgSerial - 1
			}

			fmt.Fprintf(&buf, "// %s. %s\n\n", msgCode, typename)

			buf.emit("func (*%s) NEOMsgCode() uint16 {", typename)
			buf.emit("return %s", msgCode)
			buf.emit("}\n")

			buf.WriteString(generateCodecCode(typespec, &sizer{}))
			buf.WriteString(generateCodecCode(typespec, &encoder{}))
			buf.WriteString(generateCodecCode(typespec, &decoder{}))

			msgTypeRegistry[msgCode] = typename
			msgSerial++
		}
	}

	// now generate message types registry
	buf.emit("\n// registry of message types")
	buf.emit("var msgTypeRegistry = map[uint16]reflect.Type {")

	// ordered by msgCode
	msgCodeV := []MsgCode{}
	for msgCode := range msgTypeRegistry {
		msgCodeV = append(msgCodeV, msgCode)
	}
	sort.Sort(BySerial(msgCodeV))

	for _, msgCode := range msgCodeV {
		buf.emit("%v: reflect.TypeOf(%v{}),", msgCode, msgTypeRegistry[msgCode])
	}

	buf.emit("}")

	// format & output generated code
	code, err := format.Source(buf.Bytes())
	if err != nil {
		panic(err) // should not happen
	}

	_, err = os.Stdout.Write(code)
	if err != nil {
		log.Fatal(err)
	}
}


// info about encode/decode of a basic fixed-size type
type basicCodec struct {
	wireSize int
	encode   string
	decode   string
}

var basicTypes = map[types.BasicKind]basicCodec{
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

	types.Float64:	{8, "float64_neoEncode(%v, %v)", "float64_neoDecode(%v)"},
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


// interface of a codegenerator  (for sizer/coder/decoder)
type CodeGenerator interface {
	// tell codegen it should generate code for which type & receiver name
	setFunc(recvName, typeName string, typ types.Type)

	// generate code to process a basic fixed type (not string)
	// userType is type actually used in source (for which typ is underlying), or nil
	// path is associated data member - e.g. p.Address.Port (to read from or write to)
	genBasic(path string, typ *types.Basic, userType types.Type)

	// generate code to process slice or map
	// (see genBasic for argument details)
	genSlice(path string, typ *types.Slice, obj types.Object)
	genMap(path string, typ *types.Map, obj types.Object)

	// particular case of array or slice with 1-byte elem
	//
	// NOTE this particular case is kept separate because for 1-byte
	// elements there are no byteordering issues so data can be directly
	// either accessed or copied.
	genArray1(path string, typ *types.Array)
	genSlice1(path string, typ types.Type)

	// mem.Buf
	genBuf(path string)

	// generate code for a custom type which implements its own
	// encoding/decoding via implementing neo.customCodec interface.
	genCustom(path string)

	// get generated code.
	generatedCode() string
}

// common part of codegenerators
type commonCodeGen struct {
	buf Buffer // code is emitted here

	recvName string // receiver/type for top-level func
	typeName string // or empty
	typ      types.Type

	varUsed map[string]bool // whether a variable was used
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
	num   int      // numeric part of size
	exprv []string // symbolic part of size
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

// is it numeric only?
func (s *SymSize) IsNumeric() bool {
	return len(s.exprv) == 0
}

func (s *SymSize) Reset() {
	*s = SymSize{}
}


// decoder overflow check state
type OverflowCheck struct {
	// accumulator for size to check at overflow check point
	checkSize SymSize

	// whether overflow was already checked for current decodings
	// (if yes, checkSize updates will be ignored)
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

// Add and AddExpr update .checkSize accordingly, but only if overflow was not
// already marked as checked
func (o *OverflowCheck) Add(n int) {
	if !o.checked {
		o.checkSize.Add(n)
	}
}

func (o *OverflowCheck) AddExpr(format string, a ...interface{}) {
	if !o.checked {
		o.checkSize.AddExpr(format, a...)
	}
}


// sizer generates code to compute encoded size of a message
//
// when type is recursively walked, for every case symbolic size is added appropriately.
// in case when it was needed to generate loops, runtime accumulator variable is additionally used.
// result is: symbolic size + (optionally) runtime accumulator.
type sizer struct {
	commonCodeGen
	size SymSize // currently accumulated size
}

// encoder generates code to encode a message
//
// when type is recursively walked, for every case code to update `data[n:]` is generated.
// no overflow checks are generated as by neo.Msg interface provided data
// buffer should have at least payloadLen length returned by NEOMsgEncodedLen()
// (the size computed by sizer).
//
// the code emitted looks like:
//
//	encode<typ1>(data[n1:], path1)
//	encode<typ2>(data[n2:], path2)
//	...
//
// TODO encode have to care in NEOMsgEncode to emit preamble such that bound
// checking is performed only once (currently compiler emits many of them)
type encoder struct {
	commonCodeGen
	n int // current write position in data
}

// decoder generates code to decode a message
//
// when type is recursively walked, for every case code to decode next item from
// `data[n:]` is generated.
//
// overflow checks and nread updates are grouped and emitted so that they are
// performed in the beginning of greedy fixed-wire-size blocks - checking /
// updating as much as possible to do ahead in one go.
//
// the code emitted looks like:
//
//	if len(data) < wireSize(typ1) + wireSize(typ2) + ... {
//		goto overflow
//	}
//	<assignto1> = decode<typ1>(data[n1:])
//	<assignto2> = decode<typ2>(data[n2:])
//	...
type decoder struct {
	commonCodeGen

	// done buffer for generated code
	// current delayed overflow check will be inserted in between bufDone & buf
	bufDone Buffer

	n        int   // current read position in data.
	nread    int   // numeric part of total nread return
	nreadStk []int // stack to push/pop nread on loops

	// overflow check state and size that will be checked for overflow at
	// current overflow check point
	overflow OverflowCheck
}

var _ CodeGenerator = (*sizer)(nil)
var _ CodeGenerator = (*encoder)(nil)
var _ CodeGenerator = (*decoder)(nil)


func (s *sizer) generatedCode() string {
	code := Buffer{}
	// prologue
	code.emit("func (%s *%s) NEOMsgEncodedLen() int {", s.recvName, s.typeName)
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
	code.emit("func (%s *%s) NEOMsgEncode(data []byte) {", e.recvName, e.typeName)

	code.Write(e.buf.Bytes())

	// epilogue
	code.emit("}\n")

	return code.String()
}

// data = data[n:]
// n    = 0
func (d *decoder) resetPos() {
	if d.n != 0 {
		d.emit("data = data[%v:]", d.n)
		d.n = 0
	}
}

// mark current place for insertion of overflow check code
//
// The check will be actually inserted later.
//
// later: because first we go forward in decode path scanning ahead as far as
// we can - until first seeing variable-size encoded something, and then -
// knowing fixed size would be to read - insert checking condition for
// accumulated size to here-marked overflow checkpoint.
//
// so overflowCheck does:
// 1. emit overflow checking code for previous overflow checkpoint
// 2. mark current place as next overflow checkpoint to eventually emit
//
// it has to be inserted
// - before reading a variable sized item
// - in the beginning of a loop inside	(via overflowCheckLoopEntry)
// - right after loop exit		(via overflowCheckLoopExit)
func (d *decoder) overflowCheck() {
	// nop if we know overflow was already checked
	if d.overflow.checked {
		return
	}

	//d.bufDone.emit("// overflow check point")
	if !d.overflow.checkSize.IsZero() {
		lendata := "len(data)"
		if !d.overflow.checkSize.IsNumeric() {
			// symbolic checksize has uint64 type
			lendata = "uint64(" + lendata + ")"
		}
		d.bufDone.emit("if %s < %v { goto overflow }", lendata, &d.overflow.checkSize)

		// if size for overflow check was only numeric - just
		// accumulate it at generation time
		//
		// otherwise accumulate into var(nread) at runtime.
		// we do not break runtime accumulation into numeric & symbolic
		// parts, because just above whole expression num + symbolic
		// was given to compiler as a whole so compiler should have it
		// just computed fully.
		// XXX recheck ^^^ is actually good with the compiler
		if d.overflow.checkSize.IsNumeric() {
			d.nread += d.overflow.checkSize.num
		} else {
			d.bufDone.emit("%v += %v", d.var_("nread"), &d.overflow.checkSize)
		}
	}

	d.overflow.checkSize.Reset()

	d.bufDone.Write(d.buf.Bytes())
	d.buf.Reset()
}

// overflowCheck variant that should be inserted at the beginning of a loop inside
func (d *decoder) overflowCheckLoopEntry() {
	if d.overflow.checked {
		return
	}

	d.overflowCheck()

	// upon entering a loop organize new nread, because what will be statically
	// read inside loop should be multiplied by loop len in parent context.
	d.nreadStk = append(d.nreadStk, d.nread)
	d.nread = 0
}

// overflowCheck variant that should be inserted right after loop exit
func (d *decoder) overflowCheckLoopExit(loopLenExpr string) {
	if d.overflow.checked {
		return
	}

	d.overflowCheck()

	// merge-in numeric nread updates from loop
	if d.nread != 0 {
		d.emit("%v += %v * %v", d.var_("nread"), loopLenExpr, d.nread)
	}
	l := len(d.nreadStk)
	d.nread = d.nreadStk[l-1]
	d.nreadStk = d.nreadStk[:l-1]
}



func (d *decoder) generatedCode() string {
	// flush for last overflow check point
	d.overflowCheck()

	code := Buffer{}
	// prologue
	code.emit("func (%s *%s) NEOMsgDecode(data []byte) (int, error) {", d.recvName, d.typeName)
	if d.varUsed["nread"] {
		code.emit("var %v uint64", d.var_("nread"))
	}

	code.Write(d.bufDone.Bytes())

	// epilogue
	retexpr := fmt.Sprintf("%v", d.nread)
	if d.varUsed["nread"] {
		// casting nread to int is ok even on 32 bit arches:
		// if nread would overflow 32 bits it would be caught earlier,
		// because on 32 bit arch len(data) is also 32 bit and in generated
		// code len(data) is checked first to be less than encoded message.
		retexpr += fmt.Sprintf(" + int(%v)", d.var_("nread"))
	}
	code.emit("return %v, nil", retexpr)

	// `goto overflow` is not used only for empty structs
	// NOTE for >0 check actual X in StdSizes{X} does not particularly matter
	if (&types.StdSizes{8, 8}).Sizeof(d.typ) > 0 {
		code.emit("\noverflow:")
		code.emit("return 0, ErrDecodeOverflow")
	}
	code.emit("}\n")

	return code.String()
}

// emit code to size/encode/decode basic fixed type
func (s *sizer) genBasic(path string, typ *types.Basic, userType types.Type) {
	basic := basicTypes[typ.Kind()]
	s.size.Add(basic.wireSize)
}

func (e *encoder) genBasic(path string, typ *types.Basic, userType types.Type) {
	basic := basicTypes[typ.Kind()]
	dataptr := fmt.Sprintf("data[%v:]", e.n)
	if userType != typ && userType != nil {
		// userType is a named type over some basic, like
		// type ClusterState int32
		// -> need to cast
		path = fmt.Sprintf("%v(%v)", typeName(typ), path)
	}
	e.emit(basic.encode, dataptr, path)
	e.n += basic.wireSize
}

func (d *decoder) genBasic(assignto string, typ *types.Basic, userType types.Type) {
	basic := basicTypes[typ.Kind()]

	// XXX specifying :hi is not needed - it is only a workaround to help BCE.
	//     see https://github.com/golang/go/issues/19126#issuecomment-358743715
	dataptr := fmt.Sprintf("data[%v:%v+%d]", d.n, d.n, basic.wireSize)

	decoded := fmt.Sprintf(basic.decode, dataptr)
	if userType != typ && userType != nil {
		// need to cast (like in encode case)
		decoded = fmt.Sprintf("%v(%v)", typeName(userType), decoded)
	}
	// NOTE no space before "=" - to be able to merge with ":"
	// prefix and become defining assignment
	d.emit("%s= %s", assignto, decoded)

	d.n += basic.wireSize
	d.overflow.Add(basic.wireSize)
}

// emit code to size/encode/decode array with sizeof(elem)==1
// [len(A)]byte
func (s *sizer) genArray1(path string, typ *types.Array) {
	s.size.Add(int(typ.Len()))
}

func (e *encoder) genArray1(path string, typ *types.Array) {
	e.emit("copy(data[%v:], %v[:])", e.n, path)
	e.n += int(typ.Len())
}

func (d *decoder) genArray1(assignto string, typ *types.Array) {
	typLen := int(typ.Len())
	d.emit("copy(%v[:], data[%v:%v])", assignto, d.n, d.n+typLen)
	d.n += typLen
	d.overflow.Add(typLen)
}

// emit code to size/encode/decode string or []byte
// len	u32
// [len]byte
func (s *sizer) genSlice1(path string, typ types.Type) {
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

	d.resetPos()

	d.overflowCheck()
	d.overflow.AddExpr("uint64(l)")

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

// emit code to size/encode/decode mem.Buf
// same as slice1 but buffer is allocated via mem.BufAlloc
func (s *sizer) genBuf(path string) {
	s.genSlice1(path+".XData()", nil /* typ unused */)
}

func (e *encoder) genBuf(path string) {
	e.genSlice1(path+".XData()", nil /* typ unused */)
}

func (d *decoder) genBuf(path string) {
	d.emit("{")
	d.genBasic("l:", types.Typ[types.Uint32], nil)

	d.resetPos()

	d.overflowCheck()
	d.overflow.AddExpr("uint64(l)")

	// TODO eventually do not copy but reference original
	d.emit("%v= mem.BufAlloc(int(l))", path)
	d.emit("copy(%v.Data, data[:l])", path)

	d.emit("data = data[l:]")
	d.emit("}")
}

// emit code to size/encode/decode slice
// len	u32
// [len]item
func (s *sizer) genSlice(path string, typ *types.Slice, obj types.Object) {
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
	e.emit("data = data[%v:]", e.n)
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
		d.overflowCheck()
		d.overflow.AddExpr("uint64(l) * %v", elemSize)
		d.overflow.PushChecked(true)
		defer d.overflow.PopChecked()
	}

	d.emit("%v= make(%v, l)", assignto, typeName(typ))
	d.emit("for i := 0; uint32(i) < l; i++ {")
	d.emit("a := &%s[i]", assignto)
	d.overflowCheckLoopEntry()

	codegenType("(*a)", typ.Elem(), obj, d)

	d.resetPos()
	d.emit("}")
	d.overflowCheckLoopExit("uint64(l)")

	d.emit("}")
}

// generate code to encode/decode map
// len  u32
// [len](key, value)
func (s *sizer) genMap(path string, typ *types.Map, obj types.Object) {
	keySize, keyFixed := typeSizeFixed(typ.Key())
	elemSize, elemFixed := typeSizeFixed(typ.Elem())

	if keyFixed && elemFixed {
		s.size.Add(4)
		s.size.AddExpr("len(%v) * %v", path, keySize+elemSize)
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
	e.emit("keyv := make([]%s, 0, l)", typeName(typ.Key())) // FIXME do not throw old slice away -> do xslice.Realloc()
	e.emit("for key := range %s {", path)
	e.emit("  keyv = append(keyv, key)")
	e.emit("}")
	e.emit("sort.Slice(keyv, func (i, j int) bool { return keyv[i] < keyv[j] })")

	e.emit("for _, key := range keyv {")
	codegenType("key", typ.Key(), obj, e)
	codegenType(fmt.Sprintf("%s[key]", path), typ.Elem(), obj, e)
	e.emit("data = data[%v:]", e.n) // XXX wrt map of map?
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
	if keyFixed && elemFixed {
		d.overflowCheck()
		d.overflow.AddExpr("uint64(l) * %v", keySize+elemSize)
		d.overflow.PushChecked(true)
		defer d.overflow.PopChecked()
	}

	d.emit("%v= make(%v, l)", assignto, typeName(typ))
	d.emit("m := %v", assignto)
	d.emit("for i := 0; uint32(i) < l; i++ {")
	d.overflowCheckLoopEntry()

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

	d.resetPos()
	d.emit("}")
	d.overflowCheckLoopExit("uint64(l)")

	d.emit("}")
}

// emit code to size/encode/decode custom type
func (s *sizer) genCustom(path string) {
	s.size.AddExpr("%s.neoEncodedLen()", path)
}

func (e *encoder) genCustom(path string) {
	e.emit("{")
	e.emit("n := %s.neoEncode(data[%v:])", path, e.n)
	e.emit("data = data[%v + n:]", e.n)
	e.emit("}")
	e.n = 0
}

func (d *decoder) genCustom(path string) {
	d.resetPos()

	// make sure we check for overflow previous-code before proceeding to custom decoder.
	d.overflowCheck()

	d.emit("{")
	d.emit("n, ok := %s.neoDecode(data)", path)
	d.emit("if !ok { goto overflow }")
	d.emit("data = data[n:]")
	d.emit("%v += n", d.var_("nread"))
	d.emit("}")

	// insert overflow checkpoint after custom decoder so that overflow
	// checks for following code are inserted after custom decoder call.
	d.overflowCheck()
}

// top-level driver for emitting size/encode/decode code for a type
//
// obj is object that uses this type in source program (so in case of an error
// we can point to source location for where it happened)
func codegenType(path string, typ types.Type, obj types.Object, codegen CodeGenerator) {
	// neo.customCodec
	if types.Implements(typ, neo_customCodec) ||
		types.Implements(types.NewPointer(typ), neo_customCodec) {
		codegen.genCustom(path)
		return
	}

	// mem.Buf
	if tptr, ok := typ.Underlying().(*types.Pointer); ok && tptr.Elem() == memBuf {
		codegen.genBuf(path)
		return
	}

	switch u := typ.Underlying().(type) {
	case *types.Basic:
		// go puts string into basic, but it is really slice1
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
			codegenType(path+"."+v.Name(), v.Type(), v, codegen)
		}

	case *types.Array:
		// [...]byte or [...]uint8 - just straight copy
		if typeSizeFixed1(u.Elem()) {
			codegen.genArray1(path, u)
		} else {
			var i int64
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

	case *types.Pointer:

	default:
		log.Fatalf("%v: %v has unsupported type %v (%v)", pos(obj),
			obj.Name(), typ, u)
	}
}

// generate size/encode/decode functions for a type declaration typespec
func generateCodecCode(typespec *ast.TypeSpec, codegen CodeGenerator) string {
	// type & object which refers to this type
	typ := typeInfo.Types[typespec.Type].Type
	obj := typeInfo.Defs[typespec.Name]

	codegen.setFunc("p", typespec.Name.Name, typ)
	codegenType("p", typ, obj, codegen)

	return codegen.generatedCode()
}
