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

package lonet
// registry of network hosts.

import (
	"context"
	"errors"
	"fmt"
)

// registry represents access to lonet network registry.
//
// The registry holds information about hosts available on the network, and
// for each host its OS listening address. Whenever host α needs to establish	XXX dup with lonet.go
// connection to address on host β, it queries the registry for β and further
// talks to β on that address. Correspondingly when a host joins the network,
// it announces itself to the registry so that other hosts could see it.
//
// The registry could be implemented in several ways, for example:
//
//	- dedicated network server,
//	- hosts broadcasting information to each other similar to ARP,
//	- shared memory or file,
//	- ...
type registry interface {
	// XXX + network name?

	// Announce announces host to registry.
	//
	// The host is named as hostname on lonet network and is listening for
	// incoming lonet protocol connections on OS-level osladdr address.
	//
	// Returned error, if !nil, is *registryError with .Err describing the
	// error cause:
	//
	//	- errRegistryDown  if registry cannot be accessed,	XXX (and its underlying cause?)
	//	- errHostDup       if hostname was already announced,
	//	- some other error indicating e.g. IO problem.
	Announce(ctx context.Context, hostname, osladdr string) error

	// Query queries registry for host.
	//
	// If successful - it returns OS-level network address to connect to
	// the host via lonet protocol handshake.
	//
	// Returned error, if !nil, is *registryError with .Err describing the
	// error cause:
	//
	//	- errRegistryDown  if registry cannot be accessed,	XXX ^^^
	//	- errNoHost        if hostname was not announced to registry,
	//	- some other error indicating e.g. IO problem.
	Query(ctx context.Context, hostname string) (osladdr string, _ error)

	// Close closes access to registry.
	//
	// Close interrupts all in-flight Announce and Query requests started
	// via closing registry connection. Those interrupted requests will
	// return with errRegistryDown error cause.
	Close() error
}

var errRegistryDown = errors.New("registry is down")
var errNoHost       = errors.New("no such host")
var errHostDup      = errors.New("host already registered")

// registryError represents an error of a registry operation.
type registryError struct {
	// XXX name of the network? - XXX yes
	Registry string		// name of the registry
	Op       string		// operation that failed
	Args     interface{}	// operation arguments, if any
	Err	 error		// actual error that occurred during the operation
}

func (e *registryError) Error() string {
	s := e.Registry + ": " + e.Op
	if e.Args != nil {
		s += fmt.Sprintf(" %s", e.Args)
	}
	s += ": " + e.Err.Error()
	return s
}

func (e *registryError) Cause() error {
	return e.Err
}
