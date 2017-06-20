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
	"log"
	"os"
	"sort"
	"strings"
	"text/template"

	"golang.org/x/tools/go/loader"
)

// traceEvent represents 1 trace:event definition
type traceEvent struct {
	//Pkg  loader.PackageInfo	// XXX or loader.PackageInfo.Pkg (*types.Package) ?
	//Pos  token.Position
	//Text string
	//Name string
	//Argv string
	*ast.FuncDecl
}

// TypedArgv returns argument list with types
func (te *traceEvent) TypedArgv() string {
	//format.Node(&buf, fset, te.FuncDecl.Params)

	for _, field := range te.FuncDecl.Params.List {
		namev := []string{}
		for _, name := range field.Names {
			namev = append(namev, name)
		}
	}
}

// Argv returns comma-separated argument-list
func (te *traceEvent) Argv() string {
	argv := []string{}

	for _, field := range te.FuncDecl.Params.List {
		for _, name := range field.Names {
			argv = append(argv, name)
		}
	}

	return strings.Join(argv, ", ")
}

// byEventName provides []traceEvent ordering by event name
type byEventName []*traceEvent
//func (v byEventName) Less(i, j int) bool { return v[i].Text < v[j].Text }
func (v byEventName) Less(i, j int) bool { return v[i].Name.Name < v[j].Name.Name }
func (v byEventName) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v byEventName) Len() int           { return len(v) }

// traceEventCode is code template for one trace event
const traceEventCode = `
// traceevent: {{.Name}}({{.Type.Params}})	XXX or better raw .Text (e.g. comments)

type _t_{{.Name}} struct {
	tracing.Probe
	probefunc     func{{.Type.Params}}
}

var _{{.Name}} *_t_{{.Name}}

{{/* after https://github.com/golang/go/issues/19348 is done this separate
   * checking function will be inlined and tracepoint won't cost a function
   * call when it is disabled */}}
func {{.Name}}{{.Type.Params}} {
	if _{{.Name}} != nil {
		_{{.Name}}_run({{.Type.Params}})	// XXX argv without types here
	}
}

func _{{.Name}}{{.Argv}}_run({{.Argv}}) {
	for p := _{{.Name}}; p != nil; p = (*_t_{{.Name}})(unsafe.Pointer(p.Next())) {
		p.probefunc({{.Type.Params}})	// XXX argv without types here
	}
}

// XXX ... is only types from argv
func {{.Name}}_Attach(pg *tracing.ProbeGroup, probe func({{.Type.Params}})) *tracing.Probe {
	p := _t_{{.Name}}{probefunc: probe}
	tracing.AttachProbe(pg, (**tracing.Probe)(unsafe.Pointer(&_{{.Name}}), &p.Probe)
	return &p.Probe
}
`

var traceEventCodeTmpl = template.Must(template.New("traceevent").Parse(traceEventCode))

// parseTraceEvent parses trace event definition into XXX
// text is text argument after "//trace:event "
func parseTraceEvent(text string) (*traceEvent, error) {
	if !strings.HasPrefix(text, "trace") {
		return nil, fmt.Errorf("trace event must start with \"trace\"") // XXXpos
	}

	// trace trace event definition as func declaration
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
	if declf.Type.Results != nil {
		return nil, fmt.Errorf("trace event must not return results")
	}

	return &traceEvent{declf}, nil
}

// tracegen generates code according to tracing directives in a package @ pkgpath
func tracegen(pkgpath string) error {
	// XXX  typechecking is much slower than parsing + we don't need to
	//	load anything except the package in question
	// TODO	-> use just AST parsing for loading
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

	pkg := lprog.InitialPackages()[0]
	//fmt.Println(pkg)

	eventv  := []*traceEvent{}                       // events this package defines
	importv := map[/*pkgpath*/string][]traceEvent{} // events this package imports

	// go through files of the package and process //trace: directives
	for _, file := range pkg.Files {			// ast.File
		for _, commgroup := range file.Comments {	// ast.CommentGroup
			for _, comment := range commgroup.List {// ast.Comment
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
					event, err := parseTraceEvent(arg)
					if err != nil {
						log.Fatalf("%v: %v", pos, err)
					}

					eventv = append(eventv, event)

				case "//trace:import":
					panic("TODO")
					// TODO arg is pkgpath - get trace events from tha

				default:
					log.Fatalf("%v: unknown tracing directive %q", pos, directive)
				}
			}
		}
	}

	// generate code for trace:event definitions
	sort.Sort(byEventName(eventv))
	for _, event := range eventv {
		err = traceEventCodeTmpl.Execute(os.Stdout, event)
		if err != nil {
			panic(err)
		}
	}

	// generate code for trace:import imports
	_ = importv

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
