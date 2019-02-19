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
	"log"
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
	Watchq chan<- Event
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

	drvWatchq := make(chan Event)
	drvOpt := &DriverOptions{
		ReadOnly: opt.ReadOnly,
		Watchq:   drvWatchq,
	}

	storDriver, at0, err := opener(ctx, u, drvOpt)
	if err != nil {
		return nil, err
	}

	var cache *Cache
	if !opt.NoCache {
		// small cache so that prefetch can work for loading
		// XXX 512K hardcoded (= ~ 128 · 4K-entries)
		cache = NewCache(storDriver, 128 * 4*1024)

		// FIXME teach cache for watching and remove vvv
		log.Printf("zodb: FIXME: open %s: cache is not ready for invalidations" +
			   " -> NoCache forced", zurl)
		cache = nil
	}

	// XXX stor.δtail - init with (at0, at]
	stor := &storage{
		IStorageDriver: storDriver,
		l1cache:        cache,

		drvWatchq: drvWatchq,
		drvHead:   at0,
		watchReq:  make(chan watchRequest),
		watchTab:  make(map[chan<- Event]struct{}),

	}
	go stor.watcher()	// XXX stop on close

	return stor, nil
}



// storage represents storage opened via OpenStorage.
//
// it provides a small cache on top of raw storage driver to implement prefetch
// and other storage-independed higher-level functionality.
type storage struct {
	IStorageDriver
	l1cache *Cache // can be =nil, if opened with NoCache

	// watcher
	drvWatchq chan Event                // watchq passed to driver
	drvHead   Tid                       // last tid received from drvWatchq
	watchReq  chan watchRequest         // {Add,Del}Watch requests go here
	watchTab  map[chan<- Event]struct{} // registered watchers
}

// loading goes through cache - this way prefetching can work

// XXX Close   - stop watching? (driver will close watchq in its own Close)
// XXX LastTid - report only LastTid for which cache is ready?
//		 or driver.LastTid(), then wait cache is ready?

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

// watcher
// FIXME tests

// watchRequest represents request to add/del a watch.
type watchRequest struct {
	op     watchOp      // add or del
	ack    chan Tid     // when request processed: at0 for add, ø for del.
	watchq chan<- Event // {Add,Del}Watch argument
}

type watchOp int

const (
	addWatch watchOp = 0
	delWatch watchOp = 1
)

// watcher dispatches events from driver to subscribers and serves
// {Add,Del}Watch requests.
func (s *storage) watcher() {
	// staging place for AddWatch requests.
	//
	// during event delivery to registered watchqs, add/del requests are
	// also served - not to get stuck and support clients who do DelWatch
	// and no longer receive from their watchq. However we cannot register
	// added watchq immediately, because it is undefined whether or not
	// we'll see it while iterating watchTab. So we queue what was added
	// and flush it on the beginning of each cycle.
	var addq map[chan<- Event]struct{}
	addqFlush := func() {
		for watchq := range addq {
			s.watchTab[watchq] = struct{}{}
		}
		addq = make(map[chan<- Event]struct{})
	}
	handleReq := func(req watchRequest) {
		switch req.op {
		case addWatch:
			addq[req.watchq] = struct{}{}

		case delWatch:
			delete(s.watchTab, req.watchq)
			delete(addq, req.watchq)

		default:
			panic("bad watch request op")
		}

		req.ack <- s.drvHead
	}

	// close all subscribers's watchq on close
	// XXX AddWatch/DelWatch after watcher exits?
	defer func() {
		addqFlush()
		for watchq := range s.watchTab {
			close(watchq)
		}
	}()

	for {
		addqFlush() // register staged AddWatch(s)

		select {
		case req := <-s.watchReq:
			handleReq(req)

		case event, ok := <-s.drvWatchq:
			if !ok {
				// storage closed
				return
			}

			switch event := event.(type) {
			default:
				panic(fmt.Sprintf("unexpected event: %T", event))

			case *EventError:
				// ok

			case *EventCommit:
				// XXX verify event.Tid ↑  (else e.g. δtail.Append will panic)
				//     if !↑ - stop the storage with error.
				s.drvHead = event.Tid
			}

			// deliver event to all watchers
			for watchq := range s.watchTab {
				select {
				case req := <-s.watchReq:
					handleReq(req)

				case watchq <- event:
					// ok
				}
			}
		}
	}
}

// AddWatch implements Watcher.
func (s *storage) AddWatch(watchq chan<- Event) (at0 Tid) {
	// XXX when already Closed?
	ack := make(chan Tid)
	s.watchReq <- watchRequest{addWatch, ack, watchq}
	return <-ack
}

// DelWatch implements Watcher.
func (s *storage) DelWatch(watchq chan<- Event) {
	// XXX when already Closed?
	ack := make(chan Tid)
	s.watchReq <- watchRequest{delWatch, ack, watchq}
	<-ack
}
