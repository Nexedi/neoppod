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

package zodbtools
// infrastructure to organize main driver program

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
)

// Command describes one zodb subcommand
type Command struct {
	Name	string
	Summary	string
	Usage	func (w io.Writer)
	Main	func (argv []string)
}

// CommandRegistry is ordered collection of Commands
type CommandRegistry []Command

// Lookup returns Command with corresponding name or nil
func (cmdv CommandRegistry) Lookup(command string) *Command {
	for i := range cmdv {
		if cmdv[i].Name == command {
			return &cmdv[i]
		}
	}
	return nil
}

// HelpTopic describes one help topic
type HelpTopic struct {
	Name    string
	Summary string
	Text    string
}

// HelpRegistry is ordered collection of HelpTopics
type HelpRegistry []HelpTopic

// Lookup returns HelpTopic with corresponding name or nil
func (helpv HelpRegistry) Lookup(topic string) *HelpTopic {
	for i := range helpv {
		if helpv[i].Name == topic {
			return &helpv[i]
		}
	}
	return nil
}

// ----------------------------------------

// MainProg defines a program to run with subcommands and help topics.
type MainProg struct {
	Name       string		// name of the program, e.g. "zodb"
	Summary    string		// 1-line summary of what program does
	Commands   CommandRegistry	// provided subcommands
	HelpTopics HelpRegistry		// provided help topics
}

// Exit is like os.Exit but makes sure deferred functions are run.
// Exit should be called from main goroutine.
func Exit(code int) {
	panic(&programExit{code})
}

// Fatal is like log.Fatal but makes sure deferred functions are run.
// Fatal should be called from main goroutine.
func Fatal(v ...interface{}) {
	log.Print(v...)
	Exit(1)
}

// programExit is thrown when Exit or Fatal are called
type programExit struct {
	code int
}

// Main is the main entry point for the program. Call it from main.
//
// Do not call os.Exit or log.Fatal from your program. Instead use Exit and
// Fatal from zodbtools package so that deferred functions setup by Main could
// be run.
func (prog *MainProg) Main() {
	// handle exit throw-requests
	defer func() {
		r := recover()
		if e, _ := r.(*programExit); e != nil {
			os.Exit(e.code)
		}
		if r != nil {
			panic(r)
		}
	}()

	prog.main()
}

func (prog *MainProg) main() {
	flag.Usage = prog.usage
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile := flag.String("memprofile", "", "write memory profile to `file`")
	flag.Parse()
	argv := flag.Args()

	if len(argv) == 0 {
		prog.usage()
		Exit(2)
	}

	command := argv[0]

	// handle common options
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	defer func() {
		if *memprofile != "" {
			f, err := os.Create(*memprofile)
			if err != nil {
				Fatal("could not create memory profile: ", err)
			}
			runtime.GC() // get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				Fatal("could not write memory profile: ", err)
			}
			f.Close()
		}
	}()


	// help on a topic
	if command == "help" {
		prog.help(argv)
		return
	}

	// run subcommand
	cmd := prog.Commands.Lookup(command)
	if cmd == nil {
		fmt.Fprintf(os.Stderr, "%s: unknown subcommand \"%s\"\n", prog.Name, command)
		fmt.Fprintf(os.Stderr, "Run '%s help' for usage.\n", prog.Name)
		Exit(2)
	}

	cmd.Main(argv)
}

// usage shows usage text for whole program
func (prog *MainProg) usage() {
	w := os.Stderr
	fmt.Fprintf(w,
`%s.

Usage:

	%s [options] command [arguments]

The commands are:

`, prog.Summary, prog.Name)

	// to lalign commands & help summaries
	nameWidth := 0
	for _, cmd := range prog.Commands {
		if len(cmd.Name) > nameWidth {
			nameWidth = len(cmd.Name)
		}
	}
	for _, topic := range prog.helpTopics() {
		if len(topic.Name) > nameWidth {
			nameWidth = len(topic.Name)
		}
	}

	for _, cmd := range prog.Commands {
		fmt.Fprintf(w, "\t%-*s %s\n", nameWidth, cmd.Name, cmd.Summary)
	}

	fmt.Fprintf(w,
`

Use "%s help [command]" for more information about a command.
`, prog.Name)

	if len(prog.helpTopics()) > 0 {
		fmt.Fprintf(w,
`
Additional help topics:

`)

		for _, topic := range prog.helpTopics() {
			fmt.Fprintf(w, "\t%-*s %s\n", nameWidth, topic.Name, topic.Summary)
		}

		fmt.Fprintf(w,
`
Use "%s help [topic]" for more information about that topic.

`, prog.Name)
	}
}


// help shows general help or help for a command/topic
func (prog *MainProg) help(argv []string) {
	if len(argv) < 2 {	// help topic ...
		prog.usage()
		Exit(2)
	}

	topic := argv[1]

	// topic can either be a command name or a help topic
	command := prog.Commands.Lookup(topic)
	if command != nil {
		command.Usage(os.Stdout)
		Exit(0)
	}

	helpTopic := prog.helpTopics().Lookup(topic)
	if helpTopic != nil {
		fmt.Println(helpTopic.Text)
		Exit(0)
	}

	fmt.Fprintf(os.Stderr, "Unknown help topic `%s`.  Run '%s help'.\n", topic, prog.Name)
	Exit(2)
}

// helpTopics returns provided help topics augmented with help on common topics
// provided by zodbtools driver
func (prog *MainProg) helpTopics() HelpRegistry {
	return append(helpCommon, prog.HelpTopics...)
}

var helpCommon = HelpRegistry{
	{"options", "options common to all commands", helpOptions},
}

const helpOptions =
`Options common to all commands:

	-cpuprofile <file>	write cpu profile to <file>
	-memprofile <file>	write memory profile to <file>
`
