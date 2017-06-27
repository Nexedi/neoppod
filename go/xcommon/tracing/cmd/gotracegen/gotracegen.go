// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Open Source Initiative approved licenses and Convey
// the resulting work. Corresponding source of such a combination shall include
// the source code for all other software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

/*
gotracegen generates code according to tracing annotations and imports

gotracegen package

XXX tracepoints this package defines
XXX tracepoints this package imports
*/
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"log"
	"os"
	"sort"
	"strings"
	"text/template"

	"golang.org/x/tools/go/loader"
)

// traceEvent represents 1 trace:event directive
type traceEvent struct {
	// TODO += Pos token.Position
	Pkgi		*loader.PackageInfo

	// declaration of function to signal the event
	// the declaration is constructed on the fly via converting e.g.
	//
	//	//trace:event traceConnRecv(c *Conn, msg Msg)
	//
	// into
	//
	//	func traceConnRecv(c *Conn, msg Msg)
	//
	// the func declaration is not added anywhere in the sources - just its AST is
	// constructed.	XXX + types
	*ast.FuncDecl
}

// traceImport represents 1 trace:import directive
type traceImport struct {
	Pos	token.Position
	PkgPath string
}

// Package represents tracing-related information about a package
type Package struct {
	PkgPath string
	Eventv  []*traceEvent  // trace events this package defines
	Importv []*traceImport // packages this package trace imports
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

// parseTraceEvent parses trace event definition into traceEvent
// text is text argument after "//trace:event "
func parseTraceEvent(pkgi *loader.PackageInfo, text string) (*traceEvent, error) {
	if !strings.HasPrefix(text, "trace") {
		return nil, fmt.Errorf("trace event must start with \"trace\"") // XXX pos
	}

	// trace event definition as func declaration
	text = "package xxx\nfunc " + text	// XXX
	fset := token.NewFileSet()	// XXX
	filename := "tracefunc.go"	// XXX
	f, err := parser.ParseFile(fset, filename, text, 0)
	if err != nil {
		return nil, err
	}

	if len(f.Decls) != 1 {
		return nil, fmt.Errorf("trace event must be func-like")
	}

	declf, ok := f.Decls[0].(*ast.FuncDecl)
	if !ok {
		return nil, fmt.Errorf("trace event must be func-like, not %v", f.Decls[0])
	}
	// XXX ok to allow methods (declf.Recv != nil) ?
	if declf.Type.Results != nil {
		return nil, fmt.Errorf("trace event must not return results")
	}

	return &traceEvent{pkgi, declf}, nil
}

// packageTrace returns tracing information about a package
func packageTrace(lprog *loader.Program, pkgi *loader.PackageInfo) *Package {
	eventv  := []*traceEvent{}
	importv := []*traceImport{}

	// go through files of the package and process //trace: directives
	for _, file := range pkgi.Files {			 // ast.File
		for _, commgroup := range file.Comments {	 // ast.CommentGroup
			for _, comment := range commgroup.List { // ast.Comment
				pos := lprog.Fset.Position(comment.Slash)
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
					log.Fatalf("%v: invalid directive format")
				}

				directive, arg := textv[0], textv[1]
				switch directive {
				case "//trace:event":
					event, err := parseTraceEvent(pkgi, arg)
					if err != nil {
						log.Fatalf("%v: %v", pos, err)
					}

					eventv = append(eventv, event)

				case "//trace:import":
					// reject duplicate imports
					for _, imported := range importv {
						if arg == imported.PkgPath {
							log.Fatalf("%v: duplicate trace import of %v (previous at %v)", pos, arg, imported.Pos)
						}
					}
					importv = append(importv, &traceImport{Pos: pos, PkgPath: arg})

				default:
					log.Fatalf("%v: unknown tracing directive %q", pos, directive)
				}
			}
		}
	}

	sort.Sort(byEventName(eventv))
	sort.Sort(byPkgPath(importv))

	return &Package{PkgPath: pkgi.Pkg.Path(), Eventv: eventv, Importv: importv}
}

// ----------------------------------------

// TypedArgv returns argument list with types
func (te *traceEvent) TypedArgv() string {
	//format.Node(&buf, fset, te.FuncDecl.Type.Params)
	argv := []string{}

	for _, field := range te.FuncDecl.Type.Params.List {
		namev := []string{}
		for _, name := range field.Names {
			namev = append(namev, name.Name)
		}

		arg := strings.Join(namev, ", ")
		arg += " " + types.ExprString(field.Type)

		argv = append(argv, arg)
	}

	return strings.Join(argv, ", ")
}

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

// traceEventCodeTmpl is code template generated for one trace event
var traceEventCodeTmpl = template.Must(template.New("traceevent").Parse(`
// traceevent: {{.Name}}({{.TypedArgv}})	XXX better raw .Text (e.g. comments)

{{/* probe type for this trace event */ -}}
type _t_{{.Name}} struct {
	tracing.Probe
	probefunc     func({{.TypedArgv}})
}

{{/* list of probes attached (nil if nothing) */ -}}
var _{{.Name}} *_t_{{.Name}}

{{/* function which event producer calls to notify about the event
   *
   * after https://github.com/golang/go/issues/19348 is done this separate
   * checking function will be inlined and tracepoint won't cost a function
   * call when it is disabled */ -}}
func {{.Name}}({{.TypedArgv}}) {
	if _{{.Name}} != nil {
		_{{.Name}}_run({{.Argv}})
	}
}

{{/* function to notify attached probes */ -}}
func _{{.Name}}_run({{.Argv}}) {
	for p := _{{.Name}}; p != nil; p = (*_t_{{.Name}})(unsafe.Pointer(p.Next())) {
		p.probefunc({{.Argv}})
	}
}

{{/* function to attach a probe to tracepoint */ -}}
func {{.Name}}_Attach(pg *tracing.ProbeGroup, probe func({{.TypedArgv}})) *tracing.Probe {
	p := _t_{{.Name}}{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_{{.Name}}), &p.Probe)
	return &p.Probe
}
`))

// traceEventImportTmpl is code template generated for importing one trace event
// FIXME func args types must be qualified
var traceEventImportTmpl = template.Must(template.New("traceimport").Parse(`
//go:linkname {{.Pkgi.Pkg.Name}}_{{.Name}}_Attach {{.Pkgi.Pkg.Path}}.{{.Name}}_Attach
func {{.Pkgi.Pkg.Name}}_{{.Name}}_Attach(*tracing.ProbeGroup, func({{.TypedArgv}})) *tracing.Probe
`))

// XXX go123 in magic
const magic = "// Code generated by lab.nexedi.com/kirr/go123/tracing/cmd/gotrace; DO NOT EDIT.\n"

struct Buffer {
	bytes.Buffer
}

func (b *Buffer) emit(format string, argv ...interface{}) {
	fmt.Fprintf(b, format + "\n", ...argv)
}

// tracegen generates code according to tracing directives in a package @ pkgpath
func tracegen(pkgpath string) error {
	// XXX  typechecking is much slower than parsing + we don't need to
	//	load anything except the package in question
	// TODO	-> use just AST parsing for loading?
	conf := loader.Config{
		ParserMode: parser.ParseComments,
		TypeCheckFuncBodies: func(path string) bool { return false },
	}
	conf.Import(pkgpath)

	// load package + all its imports
	lprog, err := conf.Load()
	if err != nil {
		log.Fatal(err)
	}

	// tracing info for this specified package
	pkgi := lprog.InitialPackages()[0]
	pkg := packageTrace(lprog, pkgi)

	buf := &bytes.Buffer{}

	// prologue
	buf.Write(magic)
	buf.emit("\npackage %v", pkg.Pkg.Name)
	buf.emit("\nimport (")
	buf.emit("\t\tlab.nexedi.com/kirr/neo/go/xcommon/tracing")


	// generate code for trace:event definitions
	for _, event := range pkg.Eventv {
		err = traceEventCodeTmpl.Execute(os.Stdout, event)
		if err != nil {
			panic(err)	// XXX
		}
	}

	// TODO export hash

	// generate code for trace:import imports
	fmt.Println()
	for _, timport := range pkg.Importv {
		fmt.Printf("// traceimport: %v\n", timport.PkgPath)

		pkgi = lprog.Package(timport.PkgPath)
		if pkgi == nil {
			// TODO do not require vvv
			log.Fatalf("%v: package %s must be also regularly imported", timport.Pos, timport.PkgPath)
		}

		pkg = packageTrace(lprog, pkgi)

		for _, event := range pkg.Eventv {
			err = traceEventImportTmpl.Execute(os.Stdout, event)
			if err != nil {
				panic(err)	// XXX
			}
		}
	}

	// TODO check export hash

	// TODO trace.s with "// empty .s so `go build` does not use -complete for go:linkname to work"

	return nil	// XXX
}

func main() {
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

	err := tracegen(pkgpath)
	if err != nil {
		log.Fatal(err)
	}
}
