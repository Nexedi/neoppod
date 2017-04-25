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

// Package wks links-in well-known ZODB storages
// The only purpose of this package is so that users could import it
//
//	import _ ".../zodb/wks"		XXX fixme import path
//
// and this way automatically link in support for file:// neo:// ... and other
// common storages.
package wks

import (
	_ "../../zodb/storage/fs1"
	_ "../../neo"	// XXX split into neo/client ?
)
