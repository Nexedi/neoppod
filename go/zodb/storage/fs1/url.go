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

package fs1
// open by URL support

import (
	"context"
	"fmt"
	"net/url"

	"lab.nexedi.com/kirr/neo/go/zodb"
)

func openByURL(ctx context.Context, u *url.URL, opt *zodb.OpenOptions) (zodb.IStorage, error) {
	// TODO handle query
	// XXX u.Path is not always raw path - recheck and fix
	path := u.Host + u.Path

	// XXX readonly stub
	// XXX place = ?
	if !opt.ReadOnly {
		return nil, fmt.Errorf("fs1: %s: TODO write mode not implemented", path)
	}

	fs, err := Open(ctx, path)
	if fs != nil {
		fs.url = u // FIXME move this inside Open
	}
	return fs, err
}

func init() {
	zodb.RegisterStorage("file", openByURL)
}
