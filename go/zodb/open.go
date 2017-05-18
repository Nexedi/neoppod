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

package zodb
// logic to open storages by URL

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// StorageOpener is a function to open a storage
type StorageOpener func (ctx context.Context, u *url.URL) (IStorage, error)

// {} scheme -> StorageOpener
var storageRegistry = map[string]StorageOpener{}

// RegisterStorage registers opener to be used for URLs with scheme
func RegisterStorage(scheme string, opener StorageOpener) {
	if _, already := storageRegistry[scheme]; already {
		panic(fmt.Errorf("ZODB URL scheme %q was already registered", scheme))
	}

	storageRegistry[scheme] = opener
}

// OpenStorage opens ZODB storage by URL
// Only URL schemes registered to zodb package are handled.
// Users should user import in storage packages they use or zodb/wks package to
// get support work well-known storages.
// Storage authors should register their storages with RegisterStorage
//
// TODO readonly
func OpenStorageURL(ctx context.Context, storageURL string) (IStorage, error) {
	// no scheme -> file://
	if !strings.Contains(storageURL, "://") {
		storageURL = "file://" + storageURL
	}

	u, err := url.Parse(storageURL)
	if err != nil {
		return nil, err
	}

	opener, ok := storageRegistry[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("zodb: URL scheme \"%s://\" not supported", u.Scheme)
	}

	return opener(ctx, u)
}