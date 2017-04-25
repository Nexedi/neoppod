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
)

func usage() {
	w := os.Stderr
	fmt.Fprintf(w,
`Neo is a tool for running NEO services and commands.

Usage:

	neo command [arguments]

The commands are:

`)
	for _, cmd := range zodbtools.AllCommands() {
		fmt.Fprintf(w, "\t%-11s %s\n", cmd.Name, cmd.Summary)
	}

	fmt.Fprintf(w,
`

Use "zodb help [command]" for more information about a command.

Additional help topics:

`)

	for _, topic := range zodbtools.AllHelpTopics() {
		fmt.Fprintf(w, "\t%-11s %s\n", topic.Name, topic.Summary)
	}

	fmt.Fprintf(w,
`
Use "zodb help [topic]" for more information about that topic.

`)
}

// TODO help()

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
	cmd := zodbtools.LookupCommand(command)
	if cmd == nil {
		fmt.Fprintf(os.Stderr, "zodb: unknown subcommand \"%s\"", command)
		fmt.Fprintf(os.Stderr, "Run 'zodb help' for usage.")
		os.Exit(2)
	}

	cmd.Main(argv)
}
