// Copyright (C) 2017-2019  Nexedi SA and Contributors.
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

package zodb
// open storages by URL

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"lab.nexedi.com/kirr/go123/mem"
)

// OpenOptions describes options for OpenStorage.
type OpenOptions struct {
	ReadOnly bool // whether to open storage as read-only
	NoCache  bool // don't use cache for read/write operations; prefetch will be noop
}

// DriverOptions describes options for DriverOpener.
type DriverOptions struct {
	ReadOnly bool // whether to open storage as read-only

	// Channel where storage events have to be delivered.
	//
	// Watchq can be nil to ignore such events. However if Watchq != nil, the events
	// have to be consumed or else the storage driver will misbehave - e.g.
	// it can get out of sync with the on-disk database file.
	//
	// The storage driver closes !nil Watchq when the driver is closed.
	//
	// The storage driver will send only and all events in (at₀, +∞] range,
	// where at₀ is at returned by driver open.
	//
	// TODO extend watchq to also receive errors from watcher.
	Watchq chan<- CommitEvent
}

// DriverOpener is a function to open a storage driver.
//
// at₀ gives database state at open time. The driver will send to Watchq (see
// DriverOptions) only and all events in (at₀, +∞] range.
type DriverOpener func (ctx context.Context, u *url.URL, opt *DriverOptions) (_ IStorageDriver, at0 Tid, _ error)

// {} scheme -> DriverOpener
var driverRegistry = map[string]DriverOpener{}

// RegisterDriver registers opener to be used for URLs with scheme.
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
func OpenStorage(ctx context.Context, zurl string, opt *OpenOptions) (IStorage, error) {
	// no scheme -> file://
	if !strings.Contains(zurl, "://") {
		zurl = "file://" + zurl
	}

	u, err := url.Parse(zurl)
	if err != nil {
		return nil, err
	}

	// XXX commonly handle some options from url -> opt?
	// (e.g. ?readonly=1 -> opt.ReadOnly=true + remove ?readonly=1 from URL)
	// ----//---- nocache

	opener, ok := driverRegistry[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("zodb: URL scheme \"%s://\" not supported", u.Scheme)
	}

	drvOpt := &DriverOptions{
		ReadOnly: opt.ReadOnly,
		Watchq:   nil,          // TODO use watchq to implement high-level watching
	}

	storDriver, _, err := opener(ctx, u, drvOpt) // TODO use at0 to initialize watcher δtail
	if err != nil {
		return nil, err
	}

	var cache *Cache
	if !opt.NoCache {
		// small cache so that prefetch can work for loading
		// XXX 512K hardcoded (= ~ 128 · 4K-entries)
		cache = NewCache(storDriver, 128 * 4*1024)
	}

	return &storage{
		IStorageDriver: storDriver,
		l1cache:        cache,
	}, nil
}



// storage represents storage opened via OpenStorage.
//
// it provides a small cache on top of raw storage driver to implement prefetch
// and other storage-independed higher-level functionality.
type storage struct {
	IStorageDriver
	l1cache *Cache // can be =nil, if opened with NoCache
}


// loading goes through cache - this way prefetching can work

func (s *storage) Load(ctx context.Context, xid Xid) (*mem.Buf, Tid, error) {
	// XXX here: offload xid validation from cache and driver ?
	// XXX here: offload wrapping err -> OpError{"load", err} ?
	if s.l1cache != nil {
		return s.l1cache.Load(ctx, xid)
	} else {
		return s.IStorageDriver.Load(ctx, xid)
	}
}

func (s *storage) Prefetch(ctx context.Context, xid Xid) {
	if s.l1cache != nil {
		s.l1cache.Prefetch(ctx, xid)
	}
}
