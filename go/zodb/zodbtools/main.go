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

// Package zodbtools provides tools for managing ZODB databases
package zodbtools

import "lab.nexedi.com/kirr/go123/prog"

// registry of all zodbtools commands
var commands = prog.CommandRegistry{
	// NOTE the order commands are listed here is the order how they will appear in help
	// TODO analyze ?
	// TODO cmp
	{"info",   infoSummary,   infoUsage,   infoMain},
	{"dump",   dumpSummary,   dumpUsage,   dumpMain},
	{"catobj", catobjSummary, catobjUsage, catobjMain},
}

// main zodbtools driver
var Prog = prog.MainProg{
	Name:       "zodb",
	Summary:    "Zodb is a tool for managing ZODB databases",
	Commands:   commands,
	HelpTopics: helpTopics,
}
