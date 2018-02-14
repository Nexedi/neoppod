// Copyright (C) 2018  Nexedi SA and Contributors.
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

// Package storage provides infrastructure common to all NEO storage backends.
package storage

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"sync"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

// Backend is the interface for actual storage service that is used by neo.Storage node.
type Backend interface {
	// LastTid should return the id of the last committed transaction.
	//
	// XXX same as in zodb.IStorageDriver
	// XXX +viewAt ?
	LastTid(ctx context.Context) (zodb.Tid, error)

	// LastOid should return the max object id stored.
	LastOid(ctx context.Context) (zodb.Oid, error)

	// Load, similarly to zodb.IStorageDriver.Load should load object data addressed by xid.
	// FIXME kill nextSerial support after neo/py cache does not depend on next_serial
	// XXX +viewAt ?
	Load(ctx context.Context, xid zodb.Xid) (buf *mem.Buf, serial, nextSerial zodb.Tid, err error)
}

// BackendOpener is a function to open a NEO storage backend
type BackendOpener func (ctx context.Context, u *url.URL) (Backend, error)

// {} scheme -> BackendOpener
var backMu sync.Mutex
var backRegistry = map[string]BackendOpener{}

// RegisterBackend registers opener to be used for backend URLs with scheme.
func RegisterBackend(scheme string, opener BackendOpener) {
	backMu.Lock()
	defer backMu.Unlock()

	if _, already := backRegistry[scheme]; already {
		panic(fmt.Errorf("NEO storage backend with scheme %q was already registered", scheme))
	}

	backRegistry[scheme] = opener
}

// AvailableBackends returns list of all backend schemes registered.
//
// the returned list is sorted.
func AvailableBackends() []string {
	backMu.Lock()
	defer backMu.Unlock()

	var backv []string
	for back := range backRegistry {
		backv = append(backv, back)
	}
	sort.Strings(backv)

	return backv
}


// OpenBackend opens NEO storage backend by URL.
func OpenBackend(ctx context.Context, backendURL string) (Backend, error) {
	u, err := url.Parse(backendURL)
	if err != nil {
		return nil, err
	}

	backMu.Lock()
	defer backMu.Unlock()

	opener, ok := backRegistry[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("neo: backend: URL scheme \"%s://\" not supported", u.Scheme)
	}

	return opener(ctx, u)
}
