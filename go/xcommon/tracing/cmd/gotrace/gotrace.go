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
Gotrace is a program to support and interact with go tracing subsystem.

Gotrace is a common entry to tracing and provides several subcommands:

	gen	generate code according to tracing annotations and imports
	list	lists tracepoints defined by a package

See package lab.nexedi.com/kirr/go123/tracing documentation on how to define
and use trace events in programs.

TODO automatically turn every trace:event into an USDT probe so that they can
be traced from outside of the process too.
See e.g. https://github.com/iovisor/bcc/issues/327 for context.

FIXME build tags not taken into account
*/
package main

import (
	"bufio"
	"crypto/sha1"
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"golang.org/x/tools/go/loader"

	"lab.nexedi.com/kirr/go123/xerr"
	zt "lab.nexedi.com/kirr/neo/go/zodb/zodbtools"
)

// traceEvent represents 1 trace:event declaration
type traceEvent struct {
	Pos  token.Position
	Pkgt *Package // package this trace event is part of

	// declaration of function to signal the event
	// the declaration is constructed on the fly via converting e.g.
	//
	//	//trace:event traceConnRecv(c *Conn, msg Msg)
	//
	// into
	//
	//	func traceConnRecv(c *Conn, msg Msg)
	//
	// when trace:event is parsed the func declaration is not added
	// anywhere in the sources - just its AST + package is virtually
	// constructed.
	//
	// See parseTraceEvent for details.
	*ast.FuncDecl
}

// traceImport represents 1 trace:import directive
type traceImport struct {
	Pos     token.Position
	PkgName string // "" if import name was not explicitly specified
	PkgPath string
}

// traceImported represents 1 imported trace:event
type traceImported struct {
	*traceEvent                   // imported event
	ImportSpec  *traceImport      // imported via this spec
	ImporterPkg *types.Package    // from this package
	ImportedAs  map[string]string // in context where some packages are imported as named (pkgpath -> pkgname)
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
	tracePkg      *types.Package // original package augmented with ^^^
	traceTypeInfo *types.Info    // typeinfo for ^^^
}

// parseTraceEvent parses trace event definition into traceEvent.
//
// text is text argument after "//trace:event ".
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
	//println("---- 8< ----", filename)
	//println(buf.String())
	//println("---- 8< ----")
	tf, err := parser.ParseFile(p.traceFset, filename, buf.String(), 0)
	if err != nil {
		return nil, err // already has pos' as prefix
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
		return nil, err // already has pos' as prefix
	}

	return &traceEvent{Pos: pos, Pkgt: p, FuncDecl: declf}, nil
}

// parseTraceImport parses trace import directive into traceImport.
//
// text is text argument after "//trace:import ".
func (p *Package) parseTraceImport(pos token.Position, text string) (*traceImport, error) {
	// //trace:import "path/to/pkg"
	// //trace:import name "path/to/pkg"

	if len(text) == 0 {
		return nil, fmt.Errorf("%v: empty trace-import spec", pos)
	}

	pkgname := ""
	pkgqpath := text

	if !(text[0] == '"' || text[0] == '\'') {
		textv := strings.SplitN(text, " ", 2)
		if len(textv) != 2 {
			return nil, fmt.Errorf("%v: invalid trace-import spec %v", pos, text)
		}
		pkgname = textv[0]
		pkgqpath = textv[1]
	}

	// Unquote pkgqpath as regular import does
	pkgpath, err := strconv.Unquote(pkgqpath)
	if err != nil || pkgpath == "" || pkgpath[0] == '\'' {
		return nil, fmt.Errorf("%v: invalid trace-import path %v", pos, pkgqpath)
	}

	// reject duplicate imports
	for _, imported := range p.Importv {
		if pkgpath == imported.PkgPath {
			return nil, fmt.Errorf("%v: duplicate trace import of %v (previous at %v)", pos, pkgpath, imported.Pos)
		}
	}

	return &traceImport{Pos: pos, PkgName: pkgname, PkgPath: pkgpath}, nil
}

// progImporter is types.Importer that imports packages from loaded loader.Program
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

// packageTrace returns tracing information about a package
func packageTrace(prog *loader.Program, pkgi *loader.PackageInfo) (*Package, error) {
	// prepare Package with typechecker ready to typecheck trace files
	// (to get trace func argument types)
	tconf := &types.Config{
		Importer: &progImporter{prog},

		// to ignore traceXXX() calls from original package code
		IgnoreFuncBodies: true,

		// we take imports from original source file verbatim,
		// but most of them probably won't be used.
		DisableUnusedImportCheck: true,
	}

	// tfset := token.NewFileSet() // XXX ok to separate or use original package fset?
	tfset := prog.Fset
	tpkg := types.NewPackage(pkgi.Pkg.Path(), pkgi.Pkg.Name())
	tinfo := &types.Info{Types: make(map[ast.Expr]types.TypeAndValue)}

	p := &Package{
		Pkgi: pkgi,

		// XXX vvv do we need separate field for traceFset if it is = prog.Fset?
		traceFset:     tfset,
		traceChecker:  types.NewChecker(tconf, tfset, tpkg, tinfo),
		tracePkg:      tpkg,
		traceTypeInfo: tinfo,
	}

	// preload original package files into tracing package
	err := p.traceChecker.Files(p.Pkgi.Files)
	if err != nil {
		// must not happen
		panic(fmt.Errorf("%v: error rechecking original package: %v", pkgi.Pkg.Path(), err))
	}

	// go through files of the original package and process //trace: directives
	//
	// FIXME we currently don't process cgo files as go/loader passes to us
	// already preprocessed results with comments stripped, not original source.
	// Maybe in some time it will be possible to have AST of original source:
	// https://golang.org/issues/16623
	for _, file := range pkgi.Files {                        // ast.File
		for _, commgroup := range file.Comments {        // ast.CommentGroup
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
					//fmt.Println("*", textv)
					event, err := p.parseTraceEvent(file, pos, arg)
					if err != nil {
						return nil, err
					}

					// XXX needed here? - better append in parseTraceEvent
					p.Eventv = append(p.Eventv, event)

				case "//trace:import":
					imported, err := p.parseTraceImport(pos, arg)
					if err != nil {
						return nil, err
					}

					// XXX needed here? - better append in parseTraceImport
					p.Importv = append(p.Importv, imported)

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

// SplitTests splits package into main and test parts, each covering trace-related things accordingly
func (p *Package) SplitTests() (testPkg *Package) {
	__ := *p
	testPkg = &__

	// relevant for tracing are only: .Eventv & .Importv
	eventv := p.Eventv
	importv := p.Importv
	p.Eventv = nil
	p.Importv = nil
	testPkg.Eventv = nil
	testPkg.Importv = nil

	for _, e := range eventv {
		if strings.HasSuffix(e.Pos.Filename, "_test.go") {
			testPkg.Eventv = append(testPkg.Eventv, e)
		} else {
			p.Eventv = append(p.Eventv, e)
		}
	}

	for _, i := range importv {
		if strings.HasSuffix(i.Pos.Filename, "_test.go") {
			testPkg.Importv = append(testPkg.Importv, i)
		} else {
			p.Importv = append(p.Importv, i)
		}
	}

	return testPkg
}

// ----------------------------------------

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

// ArgvTyped returns argument list with types.
//
// types are qualified relative to original package
func (te *traceEvent) ArgvTyped() string {
	return te.ArgvTypedRelativeTo(te.Pkgt.tracePkg, nil)
}

// ArgvTypedRelativeTo returns argument list with types qualified relative to specified package.
//
// importedAs specifies under which name a package was imported, if name was explicitly set
func (te *traceEvent) ArgvTypedRelativeTo(pkg *types.Package, importedAs map[string]string /*pkgpath -> pkgname*/) string {
	argv := []string{}

	// default qualifier - relative to original package
	qf := func(p *types.Package) string {
		// specified package - unqualified
		if p == pkg {
			return ""
		}

		// qualify as explicitly named
		pkgname := importedAs[p.Path()]
		if pkgname != "" {
			return pkgname
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
		typ := te.Pkgt.traceTypeInfo.Types[field.Type].Type
		arg += " " + types.TypeString(typ, qf)

		argv = append(argv, arg)
	}

	return strings.Join(argv, ", ")
}

// NeedPkgv returns packages that are needed for argument types
func (te *traceEvent) NeedPkgv() []string {
	pkgset := StrSet{ /*pkgpath*/ }
	qf := func(pkg *types.Package) string {
		// if we are called - pkg is used
		pkgset.Add(pkg.Path())
		return "" // don't care
	}

	for _, field := range te.FuncDecl.Type.Params.List {
		typ := te.Pkgt.traceTypeInfo.Types[field.Type].Type
		_ = types.TypeString(typ, qf)
	}

	return pkgset.Itemv()
}

// ImportSpec returns string representation of import spec
func (ti *traceImport) ImportSpec() string {
	t := ti.PkgName
	if t != "" {
		t += " "
	}
	t += fmt.Sprintf("%q", ti.PkgPath)
	return t
}

// traceEventCodeTmpl is code template generated for one trace event
var traceEventCodeTmpl = template.Must(template.New("traceevent").Parse(`
// traceevent: {{.Name}}({{.ArgvTyped}})

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
{{/* function to attach a probe to tracepoint imported via go:linkname */ -}}
//go:linkname {{.ImportSpec.PkgName}}_{{.Name}}_Attach {{.ImportSpec.PkgPath}}.{{.Name}}_Attach
func {{.ImportSpec.PkgName}}_{{.Name}}_Attach(*tracing.ProbeGroup, func({{.ArgvTypedRelativeTo .ImporterPkg .ImportedAs}})) *tracing.Probe
`))

// traceEventImportCheckTmpl is code template generated to check consistency with one imported package
var traceEventImportCheckTmpl = template.Must(template.New("traceimportcheck").Parse(`
{{/* linking will fail if trace import code becomes out of sync wrt imported package */ -}}
// rerun "gotrace gen" if you see link failure ↓↓↓
//go:linkname {{.ImportSpec.PkgName}}_trace_exporthash {{.ImportSpec.PkgPath}}._trace_exporthash_{{.ExportHash}}
func {{.ImportSpec.PkgName}}_trace_exporthash()
func init() { {{.ImportSpec.PkgName}}_trace_exporthash() }
`))

// magic begins all files generated by gotrace
const magic = "// Code generated by lab.nexedi.com/kirr/go123/tracing/cmd/gotrace; DO NOT EDIT.\n"

// checkCanWrite checks whether it is safe to write to file at path.
//
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

// Program represents loaded program for tracepoint analysis.
//
// It is generalization of loader.Program due to loader not allowing to
// construct programs incrementally.
type Program struct {
	// list of loader.Programs in use
	//
	// We generally need to have several programs because a package can
	// trace:import another package which is not otherwise imported by
	// original program.
	//
	// Since go/loader does not support incrementally augmenting loaded
	// program with more packages, we work-around it with having several
	// progs.
	progv []*loader.Program

	// config for loading programs
	loaderConf *loader.Config
}

// NewProgram constructs new empty Program ready to load packages according to specified build context
func NewProgram(ctxt *build.Context, cwd string) *Program {
	// adjust build context to filter-out ztrace* files when discovering packages
	//
	// we don't load what should be generated by us for 2 reasons:
	// - code generated could be wrong with older version of the
	//   tool - it should not prevent from regenerating.
	// - generated code imports packages which might be not there
	//   yet in gopath (lab.nexedi.com/kirr/go123/tracing)
	ctxtReadDir := ctxt.ReadDir
	if ctxtReadDir == nil {
		ctxtReadDir = ioutil.ReadDir
	}
	ctxtNoZTrace := *ctxt
	ctxtNoZTrace.ReadDir = func(dir string) ([]os.FileInfo, error) {
		fv, err := ctxtReadDir(dir)
		okv := fv[:0]
		for _, f := range fv {
			if !strings.HasPrefix(f.Name(), "ztrace") {
				okv = append(okv, f)
			}
		}
		return okv, err
	}

	p := &Program{}
	p.loaderConf = &loader.Config{
		ParserMode:          parser.ParseComments,
		TypeCheckFuncBodies: func(path string) bool { return false },
		Build:               &ctxtNoZTrace,
		Cwd:                 cwd,
	}

	return p
}

// Import imports a package and returns associated package info and program
// under which it was loaded.
func (p *Program) Import(pkgpath string) (prog *loader.Program, pkgi *loader.PackageInfo, err error) {
	// let's see - maybe it is already there
	for _, prog := range p.progv {
		pkgi := prog.Package(pkgpath)
		if pkgi != nil {
			return prog, pkgi, nil
		}
	}

	// not found - we have to load new program rooted at pkgpath
	p.loaderConf.ImportPkgs = nil
	p.loaderConf.Import(pkgpath)

	prog, err = p.loaderConf.Load()
	if err != nil {
		return nil, nil, err
	}

	if !(len(prog.Created) == 0 && len(prog.Imported) == 1) {
		panic("import")
	}

	p.progv = append(p.progv, prog)
	pkgi = prog.InitialPackages()[0]
	return prog, pkgi, nil
}

// ImportWithTests imports a package augmented with code from _test.go files +
// imports external test package (if present).
func (p *Program) ImportWithTests(pkgpath string) (prog *loader.Program, pkgi *loader.PackageInfo, xtestPkgi *loader.PackageInfo, err error) {
	// NOTE always reimporting not to interfere with regular imports
	p.loaderConf.ImportPkgs = nil
	p.loaderConf.ImportWithTests(pkgpath)

	prog, err = p.loaderConf.Load()
	if err != nil {
		return nil, nil, nil, err
	}

	if len(prog.Imported) != 1 {
		panic("import with tests")
	}

	if len(prog.Created) > 0 {
		xtestPkgi = prog.Created[0]
	}
	for _, pkgi = range prog.Imported {
	}

	return prog, pkgi, xtestPkgi, nil
}

// ---- `gotrace gen` ----

// tracegen generates code according to tracing directives in a package @ pkgpath.
//
// ctxt is build context for discovering packages
// cwd is "current" directory for resolving local imports (e.g. packages like "./some/package")
func tracegen(pkgpath string, ctxt *build.Context, cwd string) error {
	P := NewProgram(ctxt, cwd)

	lprog, pkgi, xtestPkgi, err := P.ImportWithTests(pkgpath)
	if err != nil {
		return err
	}

	// determine package directory
	if len(pkgi.Files) == 0 {
		return fmt.Errorf("package %s is empty", pkgi.Pkg.Path)
	}

	pkgdir := filepath.Dir(lprog.Fset.File(pkgi.Files[0].Pos()).Name())

	// tracing info for this specified package
	tpkg, err := packageTrace(lprog, pkgi)
	if err != nil {
		return err // XXX err ctx
	}

	// split everything related to tracing into plain and test (not xtest) packages
	testTpkg := tpkg.SplitTests()

	err1 := tracegen1(P, tpkg, pkgdir, "")
	err2 := tracegen1(P, testTpkg, pkgdir, "_test")

	// also handle xtest package
	xtestTpkg := &Package{} // dummy package with empty .Eventv & .Importv
	if xtestPkgi != nil {
		xtestTpkg, err = packageTrace(lprog, xtestPkgi)
		if err != nil {
			return err // XXX err ctx
		}
	}

	err3 := tracegen1(P, xtestTpkg, pkgdir, "_x_test")

	return xerr.Merge(err1, err2, err3)
}

// tracegen1 generates code according to tracing directives for a (sub)package @pkgpath.
//
// subpackage is either original package, testing code, or external test package
func tracegen1(P *Program, tpkg *Package, pkgdir string, kind string) error {
	var err error

	// write ztrace.go with code generated for trace events and imports
	ztrace_go := filepath.Join(pkgdir, "ztrace"+kind+".go")
	if len(tpkg.Eventv) == 0 && len(tpkg.Importv) == 0 {
		err = removeFile(ztrace_go)
		if err != nil {
			return err
		}
	} else {
		// prologue
		prologue := &Buffer{}
		prologue.WriteString(magic)
		prologue.emit("\npackage %v", tpkg.Pkgi.Pkg.Name())
		prologue.emit("// code generated for tracepoints")
		prologue.emit("\nimport (")
		prologue.emit("\t%q", "lab.nexedi.com/kirr/neo/go/xcommon/tracing")

		// pkgpaths of all packages needed for used types
		needPkg := StrSet{}

		// some packages are imported with explicit name
		importedAs := map[string]string{} // pkgpath -> pkgname

		text := &Buffer{}

		// code for trace:event definitions
		for _, event := range tpkg.Eventv {
			needPkg.Add("unsafe") // used in tr
			needPkg.Add(event.NeedPkgv()...)
			err = traceEventCodeTmpl.Execute(text, event)
			if err != nil {
				panic(err)
			}
		}

		// export hash symbol so that if importing package is out of
		// sync - it will have it different and linking will fail.
		if len(tpkg.Eventv) > 0 {
			text.emit("\n// trace export signature")
			//text.emit("---- 8< ----")
			//fmt.Fprintf(text, "%s", traceExport(tpkg, kind))
			//text.emit("---- 8< ----")
			text.emit("func _trace_exporthash_%s() {}", traceExportHash(tpkg, kind))
		}

		// code for trace:import imports
		for _, timport := range tpkg.Importv {
			text.emit("\n// traceimport: %s", timport.ImportSpec())

			impProg, impPkgi, err := P.Import(timport.PkgPath)
			if err != nil {
				return fmt.Errorf("%v: error trace-importing %s: %v", timport.Pos, timport.PkgPath, err)
			}

			// set name of the package if it was not explicitly specified
			if timport.PkgName == "" {
				timport.PkgName = impPkgi.Pkg.Name()
			} else {
				importedAs[timport.PkgPath] = timport.PkgName
			}

			impPkg, err := packageTrace(impProg, impPkgi)
			if err != nil {
				return err // XXX err ctx
			}

			if len(impPkg.Eventv) == 0 {
				return fmt.Errorf("%v: package %v does not export anything trace-related", timport.Pos, timport.PkgPath)
			}

			// verify export hash so link fails if it gets out of sync with imported package
			err = traceEventImportCheckTmpl.Execute(text, struct {
				ImportSpec *traceImport
				ExportHash string
			}{
				timport,
				traceExportHash(impPkg, "" /*regular package*/)})

			text.emit("")

			// import individual events
			for _, event := range impPkg.Eventv {
				needPkg.Add(event.NeedPkgv()...)
				importedEvent := traceImported{
					traceEvent:  event,
					ImportSpec:  timport,
					ImporterPkg: tpkg.Pkgi.Pkg,
					ImportedAs:  importedAs,
				}
				err = traceEventImportTmpl.Execute(text, importedEvent)
				if err != nil {
					panic(err)
				}
			}
		}

		// finish prologue with needed imports
		if !needPkg.Has("unsafe") {
			// we need it anyway because go:linkname is not allowed without unsafe
			prologue.emit("\t_ %q", "unsafe")
		} else {
			prologue.emit("\t%q", "unsafe")
			needPkg.Delete("unsafe")
		}

		needPkg.Delete(tpkg.Pkgi.Pkg.Path()) // our pkg - no need to import
		needPkgv := needPkg.Itemv()
		if len(needPkgv) > 0 {
			prologue.emit("")
		}

		for _, needpkg := range needPkgv {
			pkgname := importedAs[needpkg]
			if pkgname != "" {
				pkgname += " "
			}
			prologue.emit("\t%s%q", pkgname, needpkg)
		}
		prologue.emit(")")

		// write output to ztrace.go
		fulltext := append(prologue.Bytes(), text.Bytes()...)
		err = writeFile(ztrace_go, fulltext)
		if err != nil {
			return err
		}
	}

	// write empty ztrace.s so go:linkname works, if there are trace imports
	ztrace_s := filepath.Join(pkgdir, "ztrace"+kind+".s")
	if len(tpkg.Importv) == 0 {
		err = removeFile(ztrace_s)
	} else {
		text := &Buffer{}
		text.WriteString(magic)
		text.emit("// empty .s so `go build` does not use -complete for go:linkname to work")

		err = writeFile(ztrace_s, text.Bytes())
	}

	if err != nil {
		return err
	}

	return nil
}

// traceExport returns signatures of all tracing-related exports of a package
// in canonical order as would be seen from universe scope.
func traceExport(tpkg *Package, kind string) []byte {
	pkgpath := tpkg.Pkgi.Pkg.Path()
	pkgname := tpkg.Pkgi.Pkg.Name()

	exported := &Buffer{}
	exported.emit("%q %q", pkgpath, kind)

	for _, event := range tpkg.Eventv {
		importedEvent := traceImported{
			traceEvent:  event,
			ImportSpec:  &traceImport{PkgName: pkgname, PkgPath: pkgpath},
			ImporterPkg: nil, // from nowhere
			ImportedAs:  nil, // no naming for imports
		}
		err := traceEventImportTmpl.Execute(exported, importedEvent)
		if err != nil {
			panic(err)
		}
	}

	return exported.Bytes()
}

// traceExportHash computes signature of tracing-related exports of a package
// implementation note: it is sha1 of associated header + importing code as
// if it was executed from universe scope.
func traceExportHash(tpkg *Package, kind string) string {
	return fmt.Sprintf("%x", sha1.Sum(traceExport(tpkg, kind)))
}

const genSummary = "generate code according to tracing annotations and imports"

func genUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: gotrace gen <package>
Generate code according to tracing annotations and imports

  options:

        -h --help       this help text.
`)
}

func genMain(argv []string) {
	flags := flag.FlagSet{Usage: func() { genUsage(os.Stderr) }}
	flags.Init("", flag.ExitOnError)
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		zt.Exit(2)
	}
	pkgpath := argv[0]

	cwd, err := os.Getwd()
	if err != nil {
		zt.Fatal(err)
	}

	err = tracegen(pkgpath, &build.Default, cwd)
	if err != nil {
		zt.Fatal(err)
	}
}


// ---- `gotrace list` ----

// tracelist lists trace-events defined by a package @ pkgpath.
//
// ctxt and cwd are tunables for discovering packages. See tracegen for details.
//
// TODO support listing by pkgspec (e.g. "./...")
func tracelist(w io.Writer, pkgpath string, ctxt *build.Context, cwd string) error {
	P := NewProgram(ctxt, cwd)

	// NOTE only listing trace-events provided by main package, not tests or xtest
	lprog, pkgi, err := P.Import(pkgpath)
	if err != nil {
		return err
	}

	tpkg, err := packageTrace(lprog, pkgi)
	if err != nil {
		return err // XXX err ctx
	}

	for _, event := range tpkg.Eventv {
		_, err = fmt.Fprintf(w, "%s:%s\n", event.Pkgt.Pkgi.Pkg.Path(), event.Name)
		if err != nil {
			return err
		}
	}

	return nil
}

const listSummary = "lists tracepoints defined by a package"

func listUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: gotrace list <package>
List tracepoints defined by a package

  options:

        -h --help       this help text.
`)
}

func listMain(argv []string) {
	flags := flag.FlagSet{Usage: func() { genUsage(os.Stderr) }}
	flags.Init("", flag.ExitOnError)
	flags.Parse(argv[1:])

	argv = flags.Args()
	if len(argv) < 1 {
		flags.Usage()
		zt.Exit(2)
	}
	pkgpath := argv[0]

	cwd, err := os.Getwd()
	if err != nil {
		zt.Fatal(err)
	}

	err = tracelist(os.Stdout, pkgpath, &build.Default, cwd)
	if err != nil {
		zt.Fatal(err)
	}
}

// ---- main driver ----

var commands = zt.CommandRegistry{
	{"gen",	 genSummary,  genUsage,  genMain},
	{"list", listSummary, listUsage, listMain},
}

var helpTopics = zt.HelpRegistry{
        // XXX for now empty
}

var prog = zt.MainProg{
	Name:       "gotrace",
	Summary:    "Gotrace is a program to support and interact with go tracing subsystem",
	Commands:   commands,
	HelpTopics: helpTopics,
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("gotrace: ")
	prog.Main()
}
