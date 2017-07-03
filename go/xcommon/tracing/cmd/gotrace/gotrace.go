// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
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

/*
gotrace gen generates code according to tracing annotations and imports

gotrace gen package
gotrace list package	TODO

XXX tracepoints this package defines
XXX tracepoints this package imports
*/
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"go/types"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"golang.org/x/tools/go/loader"
)

// traceEvent represents 1 trace:event definition
type traceEvent struct {
	Pkg	*Package // package this trace event is part of

	// declaration of function to signal the event
	// the declaration is constructed on the fly via converting e.g.
	//
	//	//trace:event traceConnRecv(c *Conn, msg Msg)
	//
	// into
	//
	//	func traceConnRecv(c *Conn, msg Msg)
	//
	// the func declaration is not added anywhere in the sources - just its
	// AST + package is virtually constructed.
	*ast.FuncDecl


	// XXX -> .Pkg.{traceTypeInfo,tracePkg}
//	typinfo *types.Info	// types information for ^^^ FuncDecl
//	typpkg  *types.Package	// XXX needed ? same or not same as .Pkg ?
				// XXX -> = augmented package (original package + funcs for all trace event declarations) ?
}

// traceImport represents 1 trace:import directive
type traceImport struct {
	// XXX back link to *Package?

	Pos	token.Position
	PkgPath string
}

// Package represents tracing-related information about a package
type Package struct {
	Pkgi *loader.PackageInfo // original non-augmented package

	Eventv  []*traceEvent  // trace events this package defines
	Importv []*traceImport // trace imports of other packages

	// original package is augmented with tracing code
	// information about tracing code is below:

	traceFilev []*ast.File    // files for added trace:event funcs
	traceFset  *token.FileSet // fset for ^^^

	traceChecker  *types.Checker // to typecheck ^^^
	tracePkg      *types.Package // original package augmented ^^^
	traceTypeInfo *types.Info    // typeinfo for ^^^

	// XXX tests + xtests
}


// progImporter is types.Importer that imports packages from loaded loader.Program
// TODO also import package not yet imported by prog
type progImporter struct {
	prog *loader.Program
}

func (pi *progImporter) Import(path string) (*types.Package, error) {
	pkgi := pi.prog.Package(path)
	if pkgi == nil {
		return nil, fmt.Errorf("package %q not found", path)
	}

	return pkgi.Pkg, nil
}


// parseTraceEvent parses trace event definition into traceEvent
// text is text argument after "//trace:event "
func (p *Package) parseTraceEvent(srcfile *ast.File, pos token.Position, text string) (*traceEvent, error) {
	posErr := func(format string, argv ...interface{}) error {
		return fmt.Errorf("%v: "+format, append([]interface{}{pos}, argv...)...)
	}

	if !strings.HasPrefix(text, "trace") {
		return nil, posErr("trace event must start with \"trace\"")
	}

	// prepare artificial package with trace event definition as func declaration
	buf := &Buffer{}
	buf.emit("package %s", p.Pkgi.Pkg.Name())

	// add all imports from original source file
	// so that inside it all looks like as if it was in original source context
	buf.emit("\nimport (")

	for _, imp := range srcfile.Imports {
		impline := ""
		if imp.Name != nil {
			impline += imp.Name.Name + " "
		}
		impline += imp.Path.Value
		buf.emit("\t%s", impline)
	}

	buf.emit(")")

	// func itself
	buf.emit("\nfunc " + text)

	// now parse/typecheck
	filename := fmt.Sprintf("%v:%v+trace:event %v", pos.Filename, pos.Line, text)
	//println("--------")
	//println(buf.String())
	//println("--------")
	tf, err := parser.ParseFile(p.traceFset, filename, buf.String(), 0)
	if err != nil {
		return nil, err // should already have pos' as prefix
	}

	p.traceFilev = append(p.traceFilev, tf)

	// must be:
	// GenDecl{IMPORT}
	// FuncDecl
	if len(tf.Decls) != 2 {
		return nil, posErr("trace event must be func-like")
	}

	declf, ok := tf.Decls[1].(*ast.FuncDecl)
	if !ok {
		return nil, posErr("trace event must be func-like, not %v", tf.Decls[0])
	}
	// XXX ok to allow methods (declf.Recv != nil) ?
	if declf.Type.Results != nil {
		return nil, posErr("trace event must not return results")
	}

	// typecheck prepared file to get trace func argument types
	// (type information lands into p.traceTypeInfo)
	err = p.traceChecker.Files([]*ast.File{tf})
	if err != nil {
		return nil, err // should already have pos' as prefix
	}

	return &traceEvent{Pkg: p, FuncDecl: declf}, nil
}

// packageTrace returns tracing information about a package
func packageTrace(prog *loader.Program, pkgi *loader.PackageInfo) (*Package, error) {
	// prepare Package with typechecker ready to typecheck trace files
	// (to get trace func argument types)
	tconf := &types.Config{
			Importer: &progImporter{prog},

			// we took imports from original source file verbatim,
			// but most of them probably won't be used.
			DisableUnusedImportCheck: true,
		}

	tfset := token.NewFileSet() // XXX ok to separate or use original package fset?
	tpkg  := types.NewPackage(pkgi.Pkg.Path(), pkgi.Pkg.Name())
	tinfo := &types.Info{Types: make(map[ast.Expr]types.TypeAndValue)}

	p := &Package{
		Pkgi:		pkgi,
		traceFset:	tfset,
		traceChecker:	types.NewChecker(tconf, tfset, tpkg, tinfo),
		tracePkg:	tpkg,
		traceTypeInfo:	tinfo,
	}

	// go through files of the original package and process //trace: directives
	for _, file := range pkgi.Files {			 // ast.File
		for _, commgroup := range file.Comments {	 // ast.CommentGroup
			for _, comment := range commgroup.List { // ast.Comment
				pos := prog.Fset.Position(comment.Slash)
				//fmt.Printf("%v %q\n", pos, comment.Text)

				// only directives starting from beginning of line
				if pos.Column != 1 {
					continue
				}

				if !strings.HasPrefix(comment.Text, "//trace:") {
					continue
				}

				textv := strings.SplitN(comment.Text, " ", 2)
				if len(textv) != 2 {
					return nil, fmt.Errorf("%v: invalid directive format", pos)
				}

				directive, arg := textv[0], textv[1]
				switch directive {
				case "//trace:event":
					event, err := p.parseTraceEvent(file, pos, arg)
					if err != nil {
						return nil, err
					}

					// XXX needed here? - better append in parseTraceEvent
					p.Eventv = append(p.Eventv, event)

				case "//trace:import":
					// Unqote arg as in regular import
					importPath, err := strconv.Unquote(arg)
					if err != nil || arg[0] == '\'' {
						return nil, fmt.Errorf("%v: invalid trace-import path %v", pos, arg)
					}

					// reject duplicate imports
					for _, imported := range p.Importv {
						if importPath == imported.PkgPath {
							return nil, fmt.Errorf("%v: duplicate trace import of %v (previous at %v)", pos, importPath, imported.Pos)
						}
					}
					p.Importv = append(p.Importv, &traceImport{Pos: pos, PkgPath: importPath})

				default:
					return nil, fmt.Errorf("%v: unknown tracing directive %q", pos, directive)
				}
			}
		}
	}

	// events and imports go in canonical order
	sort.Sort(byEventName(p.Eventv))
	sort.Sort(byPkgPath(p.Importv))

	return p, nil
}

// ----------------------------------------

// byEventName provides []*traceEvent ordering by event name
type byEventName []*traceEvent
func (v byEventName) Less(i, j int) bool { return v[i].Name.Name < v[j].Name.Name }
func (v byEventName) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v byEventName) Len() int           { return len(v) }

// byPkgPath provides []*traceImport ordering by package path
type byPkgPath []*traceImport
func (v byPkgPath) Less(i, j int) bool { return v[i].PkgPath < v[j].PkgPath }
func (v byPkgPath) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v byPkgPath) Len() int           { return len(v) }


// Argv returns comma-separated argument-list
func (te *traceEvent) Argv() string {
	argv := []string{}

	for _, field := range te.FuncDecl.Type.Params.List {
		for _, name := range field.Names {
			argv = append(argv, name.Name)
		}
	}

	return strings.Join(argv, ", ")
}

// ArgvTyped returns argument list with types
// types are qualified relative to original package
func (te *traceEvent) ArgvTyped() string {
	return te.ArgvTypedRelativeTo(te.Pkg.tracePkg)
}

// ArgvTypedRelativeTo returns argument list with types qualified relative to specified package
func (te *traceEvent) ArgvTypedRelativeTo(pkg *types.Package) string {
	argv := []string{}

	// default qualifier - relative to original package
	qf := func(p *types.Package) string {
		// specified package - unqualified
		if p == pkg {
			return ""
		}

		// default qualification
		return p.Name()
	}

	for _, field := range te.FuncDecl.Type.Params.List {
		namev := []string{}
		for _, name := range field.Names {
			namev = append(namev, name.Name)
		}

		arg := strings.Join(namev, ", ")
		typ := te.Pkg.traceTypeInfo.Types[field.Type].Type
		arg += " " + types.TypeString(typ, qf)

		argv = append(argv, arg)
	}

	return strings.Join(argv, ", ")
}


// NeedPkgv returns packages that are needed for argument types
func (te *traceEvent) NeedPkgv() []string {
	pkgset := StrSet{/*pkgpath*/}
	qf := func(pkg *types.Package) string {
		// if we are called - pkg is used
		pkgset.Add(pkg.Path())
		return "" // don't care
	}

	for _, field := range te.FuncDecl.Type.Params.List {
		typ := te.Pkg.traceTypeInfo.Types[field.Type].Type
		_ = types.TypeString(typ, qf)
	}

	return pkgset.Itemv()
}

// traceEventCodeTmpl is code template generated for one trace event
var traceEventCodeTmpl = template.Must(template.New("traceevent").Parse(`
// traceevent: {{.Name}}({{.ArgvTyped}})	XXX better raw .Text (e.g. comments)

{{/* probe type for this trace event */ -}}
type _t_{{.Name}} struct {
	tracing.Probe
	probefunc     func({{.ArgvTyped}})
}

{{/* list of probes attached (nil if nothing) */ -}}
var _{{.Name}} *_t_{{.Name}}

{{/* function which event producer calls to notify about the event
   *
   * after https://github.com/golang/go/issues/19348 is done this separate
   * checking function will be inlined and tracepoint won't cost a function
   * call when it is disabled */ -}}
func {{.Name}}({{.ArgvTyped}}) {
	if _{{.Name}} != nil {
		_{{.Name}}_run({{.Argv}})
	}
}

{{/* function to notify attached probes */ -}}
func _{{.Name}}_run({{.ArgvTyped}}) {
	for p := _{{.Name}}; p != nil; p = (*_t_{{.Name}})(unsafe.Pointer(p.Next())) {
		p.probefunc({{.Argv}})
	}
}

{{/* function to attach a probe to tracepoint */ -}}
func {{.Name}}_Attach(pg *tracing.ProbeGroup, probe func({{.ArgvTyped}})) *tracing.Probe {
	p := _t_{{.Name}}{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_{{.Name}})), &p.Probe)
	return &p.Probe
}
`))

// traceEventImportTmpl is code template generated for importing one trace event
var traceEventImportTmpl = template.Must(template.New("traceimport").Parse(`
//go:linkname {{.Pkg.Name}}_{{.Name}}_Attach {{.Pkg.Path}}.{{.Name}}_Attach
func {{.Pkg.Name}}_{{.Name}}_Attach(*tracing.ProbeGroup, func({{.ArgvTypedRelativeTo .ImporterPkg}})) *tracing.Probe
`))

// magic begins all files generated by gotrace
const magic = "// Code generated by lab.nexedi.com/kirr/go123/tracing/cmd/gotrace; DO NOT EDIT.\n"

// checkCanWrite checks whether it is safe to write to file at path
// it is safe to write when either
// - the file does not exist, or
// - it exits but was previously generated by us
func checkCanWrite(path string) error {
	f, err := os.Open(path)
	if e, ok := err.(*os.PathError); ok && os.IsNotExist(e.Err) {
		return nil
	}

	defer f.Close()
	bf := bufio.NewReader(f)

	headline, err := bf.ReadString('\n')
	if err != nil || headline != magic {
		return fmt.Errorf("refusing to make output: %v exists but was not generated by gotrace", path)
	}

	return nil
}

// writeFile writes data to a file at path after checking it is safe to write there
func writeFile(path string, data []byte) error {
	err := checkCanWrite(path)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, data, 0666)
}

// removeFile make sure there is no file at path after checking it is safe to write to that file
func removeFile(path string) error {
	err := checkCanWrite(path)
	if err != nil {
		return err
	}

	err = os.Remove(path)
	if e, ok := err.(*os.PathError); ok && os.IsNotExist(e.Err) {
		err = nil
	}
	return err
}

// Buffer is bytes.Buffer + syntatic sugar
type Buffer struct {
	bytes.Buffer
}

func (b *Buffer) emit(format string, argv ...interface{}) {
	fmt.Fprintf(b, format + "\n", argv...)
}


// StrSet is set<string>
type StrSet map[string]struct{}

func (s StrSet) Add(itemv ...string) {
	for _, item := range itemv {
		s[item] = struct{}{}
	}
}

func (s StrSet) Delete(item string) {
	delete(s, item)
}

// Itemv returns ordered slice of set items
func (s StrSet) Itemv() []string {
	itemv := make([]string, 0, len(s))
	for item := range s {
		itemv = append(itemv, item)
	}
	sort.Strings(itemv)
	return itemv
}

// findPackageNoZTrace is (*build.Context).Import but filters-out ztrace* files from found package
//
// we don't load what should be generated by us for 2 reasons:
// - code generated could be wrong with older version of the
//   tool - it should not prevent from regenerating.
// - generated code imports packages which might be not there
//   yet in gopath (lab.nexedi.com/kirr/go123/tracing)
func findPackageNoZTrace(ctxt *build.Context, fromDir, importPath string, mode build.ImportMode) (*build.Package, error) {
	bp, err := ctxt.Import(importPath, fromDir, mode)

	filter := func(filev *[]string) {
		okv := []string{}
		for _, f := range *filev {
			if strings.HasPrefix(f, "ztrace") {
				bp.IgnoredGoFiles = append(bp.IgnoredGoFiles, f)
			} else {
				okv = append(okv, f)
			}
		}
		*filev = okv
	}

	if bp != nil {
		filter(&bp.GoFiles)
		filter(&bp.CgoFiles)
		filter(&bp.TestGoFiles)
		filter(&bp.XTestGoFiles)
		filter(&bp.SFiles)

		// XXX also adjust .Import{s,Pos}, .TestImport{s,Pos}, .XTestImport{s,Pos} ?
	}

	return bp, err
}

// tracegen generates code according to tracing directives in a package @ pkgpath
//
// ctxt is build context for discovering packages
// cwd is "current" directory for resolving local imports (e.g. packages like "./some/package")
func tracegen(pkgpath string, ctxt *build.Context, cwd string) error {
	// TODO test-only with .TestGoFiles  .XTestGoFiles

	conf := loader.Config{
		ParserMode: parser.ParseComments,
		TypeCheckFuncBodies: func(path string) bool { return false },
		Build: ctxt,
		Cwd: cwd,
		FindPackage: findPackageNoZTrace,
	}
	conf.Import(pkgpath)

	// load package + all its imports
	lprog, err := conf.Load()
	if err != nil {
		return err
	}

	pkgi := lprog.InitialPackages()[0]

	// determine package directory
	if len(pkgi.Files) == 0 {
		return fmt.Errorf("package %s is empty", pkgi.Pkg.Path)
	}

	pkgdir := filepath.Dir(lprog.Fset.File(pkgi.Files[0].Pos()).Name())
	pkgpath = pkgi.Pkg.Path()
	//println("pkgpath", pkgpath)
	//println("pkgdir", pkgdir)
	//return nil

	// tracing info for this specified package
	tpkg, err := packageTrace(lprog, pkgi)
	if err != nil {
		return err // XXX err ctx
	}

	// prologue
	prologue := &Buffer{}
	prologue.WriteString(magic)
	prologue.emit("\npackage %v", tpkg.Pkgi.Pkg.Name())
	prologue.emit("// code generated for tracepoints")
	prologue.emit("\nimport (")
	prologue.emit("\t%q", "lab.nexedi.com/kirr/neo/go/xcommon/tracing")
	prologue.emit("\t%q", "unsafe")
	// import of all packages needed for used types will go here in the end
	needPkg := StrSet{}

	// code for trace:event definitions
	text := &Buffer{}
	for _, event := range tpkg.Eventv {
		needPkg.Add(event.NeedPkgv()...)
		err = traceEventCodeTmpl.Execute(text, event)
		if err != nil {
			panic(err)	// XXX
		}
	}

	// TODO export hash

	// code for trace:import imports
	for _, timport := range tpkg.Importv {
		text.emit("\n// traceimport: %v", timport.PkgPath)

		impPkgi := lprog.Package(timport.PkgPath)
		if impPkgi == nil {
			// TODO do not require vvv
			return fmt.Errorf("%v: package %s must be also regularly imported", timport.Pos, timport.PkgPath)
		}

		impPkg, err := packageTrace(lprog, impPkgi)
		if err != nil {
			return err // XXX err ctx
		}

		for _, event := range impPkg.Eventv {
			needPkg.Add(event.NeedPkgv()...)
			importedEvent := struct{
				*traceEvent
				ImporterPkg *types.Package
			} {event, pkgi.Pkg }
			err = traceEventImportTmpl.Execute(text, importedEvent)
			if err != nil {
				panic(err)	// XXX
			}
		}
	}

	// TODO check export hash

	// finish prologue with needed imports
	needPkg.Delete(pkgpath)	// our pkg - no need to import
	needPkgv := needPkg.Itemv()
	if len(needPkgv) > 0 {
		prologue.emit("")
	}

	for _, needpkg := range needPkgv {
		prologue.emit("\t%q", needpkg)
	}
	prologue.emit(")")

	// write output to ztrace.go
	fulltext := append(prologue.Bytes(), text.Bytes()...)
	err = writeFile(filepath.Join(pkgdir, "ztrace.go"), fulltext)
	if err != nil {
		return err
	}

	// write empty ztrace.s so go:linkname works, if there are trace imports
	ztrace_s := filepath.Join(pkgdir, "ztrace.s")
	if len(tpkg.Importv) == 0 {
		err = removeFile(ztrace_s)
	} else {
		text.Reset()
		text.WriteString(magic)
		text.emit("// empty .s so `go build` does not use -complete for go:linkname to work")

		err = writeFile(ztrace_s, text.Bytes())
	}

	if err != nil {
		return err
	}

	return nil	// XXX
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("gotrace: ")

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

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	err = tracegen(pkgpath, &build.Default, cwd)
	if err != nil {
		log.Fatal(err)
	}
}
