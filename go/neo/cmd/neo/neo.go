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

// Neo is a driver program for running & invoking NEO commands and services
package main

import (
	"flag"
	"fmt"
	"os"

	"../../../neo"
)

func usage() {
	w := os.Stderr
	fmt.Fprintf(w,
`Neo is a tool for running NEO services and commands.

Usage:

	neo command [arguments]

The commands are:

`)
	for _, cmd := range neo.Commands {
		fmt.Fprintf(w, "\t%-11s %s\n", cmd.Name, cmd.Summary)
	}

	fmt.Fprintf(w,
`

Use "neo help [command]" for more information about a command.

Additional help topics:

`)

	for _, topic := range neo.HelpTopics {
		fmt.Fprintf(w, "\t%-11s %s\n", topic.Name, topic.Summary)
	}

	fmt.Fprintf(w,
`
Use "neo help [topic]" for more information about that topic.

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
	command := neo.Commands.Lookup(topic)
	if command != nil {
		command.Usage(os.Stdout)
		os.Exit(0)
	}

	helpTopic := neo.HelpTopics.Lookup(topic)
	if helpTopic != nil {
		fmt.Println(helpTopic.Text)
		os.Exit(0)
	}

	fmt.Fprintf(os.Stderr, "Unknown help topic `%s`.  Run 'neo help'.\n", topic)
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
	cmd := neo.Commands.Lookup(command)
	if cmd == nil {
		fmt.Fprintf(os.Stderr, "neo: unknown subcommand \"%s\"", command)
		fmt.Fprintf(os.Stderr, "Run 'neo help' for usage.")
		os.Exit(2)
	}

	cmd.Main(argv)
}
