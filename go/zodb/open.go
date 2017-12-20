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

// DriverOpener is a function to open a storage driver
type DriverOpener func (ctx context.Context, u *url.URL, opt *OpenOptions) (IStorageDriver, error)

// {} scheme -> DriverOpener
var driverRegistry = map[string]DriverOpener{}

// RegisterDriver registers opener to be used for URLs with scheme
func RegisterDriver(scheme string, opener DriverOpener) {
	if _, already := driverRegistry[scheme]; already {
		panic(fmt.Errorf("ZODB URL scheme %q was already registered", scheme))
	}

	driverRegistry[scheme] = opener
}

// OpenStorage opens ZODB storage by URL.
//
// Only URL schemes registered to zodb package are handled.
// Users should import in storage packages they use or zodb/wks package to
// get support for well-known storages.
//
// Storage authors should register their storages with RegisterStorage.
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

	opener, ok := driverRegistry[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("zodb: URL scheme \"%s://\" not supported", u.Scheme)
	}

	storDriver, err := opener(ctx, u, opt)
	if err != nil {
		return nil, err
	}

	return &storage{
		IStorageDriver: storDriver,

		// small cache so that prefetch can work for loading
		l1cache: NewCache(storDriver, 16 << 20),	// XXX 16MB hardcoded
	}, nil
}



// storage represents storage opened via OpenStorage.
//
// it provides a small cache on top of raw storage driver to implement prefetch
// and other storage-independed higher-level functionality.
type storage struct {
	IStorageDriver
	l1cache *Cache
	url     string		// URL this storage was opened via
}


// loading always goes through cache - this way prefetching can work

func (s *storage) Load(ctx context.Context, xid Xid) (*Buf, Tid, error) {
	return s.l1cache.Load(ctx, xid)
}

func (s *storage) Prefetch(ctx context.Context, xid Xid) {
	s.l1cache.Prefetch(ctx, xid)
}


func (s *storage) URL() string {
	return s.url
}
