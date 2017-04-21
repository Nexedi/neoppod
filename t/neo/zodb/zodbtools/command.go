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

package zodbtools
// registry for all commands

import (
	"io"
)

// Command describes one zodb subcommand
type Command struct {
	Name	string
	Summary	string
	Usage	func (w io.Writer)
	Main	func (argv []string)
}

// registry of all commands
var cmdv = []Command{
	// NOTE the order commands are listed here is the order how they will appear in help
	// TODO analyze ?
	// TODO cmp
	{"dump", dumpSummary, dumpUsage, dumpMain},
	{"info", infoSummary, infoUsage, infoMain},
}

// LookupCommand returns Command with corresponding name or nil
func LookupCommand(command string) *Command {
	for i := range cmdv {
		if cmdv[i].Name == command {
			return &cmdv[i]
		}
	}
	return nil
}

// AllCommands returns list of all zodbtools commands
func AllCommands() []Command {
	return cmdv
}
