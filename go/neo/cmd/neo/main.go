// Copyright (C) 2017-2018  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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

// Neo is a driver program for running and managing NEO databases.
package main

import "lab.nexedi.com/kirr/go123/prog"

var commands = prog.CommandRegistry{
	{"master",  masterSummary,  masterUsage,  masterMain},
	{"storage", storageSummary, storageUsage, storageMain},
}

var helpTopics = prog.HelpRegistry{
	// XXX for now empty
}

func main() {
	prog := prog.MainProg{
	        Name:       "neo",
	        Summary:    "Neo is a tool to run NEO services and commands",
	        Commands:   commands,
	        HelpTopics: helpTopics,
	}

	prog.Main()
}
