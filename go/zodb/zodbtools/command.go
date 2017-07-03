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
// registry for all commands

import "io"

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

// registry of all zodbtools commands
var Commands = CommandRegistry{
	// NOTE the order commands are listed here is the order how they will appear in help
	// TODO analyze ?
	// TODO cmp
	{"info", infoSummary, infoUsage, infoMain},
	{"dump", dumpSummary, dumpUsage, dumpMain},
	{"catobj", catobjSummary, catobjUsage, catobjMain},
}
