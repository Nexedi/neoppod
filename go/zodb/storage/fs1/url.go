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

package fs1
// open URL support

import (
	"net/url"

	"../../../zodb"
)

// TODO read-only support
func openByURL(u *url.URL) (zodb.IStorage, error) {
	// TODO handle query
	// XXX u.Path is not always raw path - recheck and fix
	return Open(u.Host + u.Path)
}

func init() {
	zodb.RegisterStorage("file", openByURL)
}
