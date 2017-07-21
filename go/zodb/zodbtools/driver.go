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
	"os"
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

// Main is the main entry point for the program. Call it from main.
func (prog *MainProg) Main() {
	flag.Usage = prog.usage
	flag.Parse()
	argv := flag.Args()

	if len(argv) == 0 {
		prog.usage()
		os.Exit(2)
	}

	command := argv[0]

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
		os.Exit(2)
	}

	cmd.Main(argv)
}

// usage shows usage text for whole program
func (prog *MainProg) usage() {
	w := os.Stderr
	fmt.Fprintf(w,
`%s.

Usage:

	%s command [arguments]

The commands are:

`, prog.Summary, prog.Name)
	for _, cmd := range prog.Commands {
		fmt.Fprintf(w, "\t%-11s %s\n", cmd.Name, cmd.Summary)
	}

	fmt.Fprintf(w,
`

Use "%s help [command]" for more information about a command.
`, prog.Name)

	if len(prog.HelpTopics) > 0 {
		fmt.Fprintf(w,
`
Additional help topics:

`)

		for _, topic := range prog.HelpTopics {
			fmt.Fprintf(w, "\t%-11s %s\n", topic.Name, topic.Summary)
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
		os.Exit(2)
	}

	topic := argv[1]

	// topic can either be a command name or a help topic
	command := prog.Commands.Lookup(topic)
	if command != nil {
		command.Usage(os.Stdout)
		os.Exit(0)
	}

	helpTopic := prog.HelpTopics.Lookup(topic)
	if helpTopic != nil {
		fmt.Println(helpTopic.Text)
		os.Exit(0)
	}

	fmt.Fprintf(os.Stderr, "Unknown help topic `%s`.  Run '%s help'.\n", topic, prog.Name)
	os.Exit(2)
}
