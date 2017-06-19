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

// Zodb is a driver program for invoking zodbtools subcommands
package main

import (
	"flag"
	"fmt"
	"os"

	"lab.nexedi.com/kirr/neo/go/zodb/zodbtools"

	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"
)

func usage() {
	w := os.Stderr
	fmt.Fprintf(w,
`Zodb is a tool for managing ZODB databases.

Usage:

	zodb command [arguments]

The commands are:

`)
	for _, cmd := range zodbtools.Commands {
		fmt.Fprintf(w, "\t%-11s %s\n", cmd.Name, cmd.Summary)
	}

	fmt.Fprintf(w,
`

Use "zodb help [command]" for more information about a command.

Additional help topics:

`)

	for _, topic := range zodbtools.HelpTopics {
		fmt.Fprintf(w, "\t%-11s %s\n", topic.Name, topic.Summary)
	}

	fmt.Fprintf(w,
`
Use "zodb help [topic]" for more information about that topic.

`)
}


// help shows general help or help for a command/topic
func help(argv []string) {
	if len(argv) < 2 {	// help topic ...
		usage()
		os.Exit(2)
	}

	topic := argv[1]

	// topic can either be a command name or a help topic
	command := zodbtools.Commands.Lookup(topic)
	if command != nil {
		command.Usage(os.Stdout)
		os.Exit(0)
	}

	helpTopic := zodbtools.HelpTopics.Lookup(topic)
	if helpTopic != nil {
		fmt.Println(helpTopic.Text)
		os.Exit(0)
	}

	fmt.Fprintf(os.Stderr, "Unknown help topic `%s`.  Run 'zodb help'.\n", topic)
	os.Exit(2)
}

func main() {
	flag.Usage = usage
	flag.Parse()
	argv := flag.Args()

	if len(argv) == 0 {
		usage()
		os.Exit(2)
	}

	command := argv[0]

	// help on a topic
	if command == "help" {
		help(argv)
		return
	}

	// run subcommand
	cmd := zodbtools.Commands.Lookup(command)
	if cmd == nil {
		fmt.Fprintf(os.Stderr, "zodb: unknown subcommand \"%s\"\n", command)
		fmt.Fprintf(os.Stderr, "Run 'zodb help' for usage.\n")
		os.Exit(2)
	}

	cmd.Main(argv)
}
