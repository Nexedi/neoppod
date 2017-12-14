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
// registry for all help topics

import "lab.nexedi.com/kirr/go123/prog"

const helpZURL =
`Almost every zodb command works with a database.
A database can be specified by way of providing URL for its storage.

The most general way to specify a storage is via preparing file with
ZConfig-based storage definition, e.g.

    %import neo.client
    <NEOStorage>
        master_nodes    ...
        name            ...
    </NEOStorage>

and using path to that file with zconfig:// schema:

    zconfig://<path-to-zconfig-storage-definition>

There are also following simpler ways:

- neo://<db>@<master>   for a NEO database
- zeo://<host>:<port>   for a ZEO database
- /path/to/file         for a FileStorage database

Please see zodburi documentation for full details:

http://docs.pylonsproject.org/projects/zodburi/
`

const helpXid =
`An object address for loading from ZODB should be specified as follows:

	- tid
	- ":"
	- oid

for example

	0285cbac258bf266:0000000000000001	- oid 1 at first newest transaction changing it with tid <= 0285cbac258bf266
`

var helpTopics = prog.HelpRegistry{
	{"zurl",  "specifying database URL",	helpZURL},
	{"xid",   "specifying object address",	helpXid},
}
