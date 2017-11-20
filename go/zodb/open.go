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

package zodb
// open storages by URL

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// OpenOptions describes options for OpenStorage
type OpenOptions struct {
	ReadOnly bool // whether to open storage as read-only
}

// StorageOpener is a function to open a storage
type StorageOpener func (ctx context.Context, u *url.URL, opt *OpenOptions) (IStorage, error)

// {} scheme -> StorageOpener
var storageRegistry = map[string]StorageOpener{}

// RegisterStorage registers opener to be used for URLs with scheme
func RegisterStorage(scheme string, opener StorageOpener) {
	if _, already := storageRegistry[scheme]; already {
		panic(fmt.Errorf("ZODB URL scheme %q was already registered", scheme))
	}

	storageRegistry[scheme] = opener
}

// OpenStorage opens ZODB storage by URL.
//
// Only URL schemes registered to zodb package are handled.
// Users should import in storage packages they use or zodb/wks package to
// get support for well-known storages.
//
// Storage authors should register their storages with RegisterStorage.
//
// TODO automatically wrap opened storage with Cache.
func OpenStorage(ctx context.Context, storageURL string, opt *OpenOptions) (IStorage, error) {
	// no scheme -> file://
	if !strings.Contains(storageURL, "://") {
		storageURL = "file://" + storageURL
	}

	u, err := url.Parse(storageURL)
	if err != nil {
		return nil, err
	}

	// XXX commonly handle some options from url -> opt?
	// (e.g. ?readonly=1 -> opt.ReadOnly=true + remove ?readonly=1 from URL)

	opener, ok := storageRegistry[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("zodb: URL scheme \"%s://\" not supported", u.Scheme)
	}

	return opener(ctx, u, opt)
}
