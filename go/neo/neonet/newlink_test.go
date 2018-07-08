// Copyright (C) 2016-2018  Nexedi SA and Contributors.
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

package neonet

import (
	"context"
	"io"
	"net"
	"testing"

	"golang.org/x/sync/errgroup"

	"lab.nexedi.com/kirr/go123/exc"
)

func xhandshake(ctx context.Context, c net.Conn, version uint32) {
	err := handshake(ctx, c, version)
	exc.Raiseif(err)
}

func TestHandshake(t *testing.T) {
	bg := context.Background()
	// handshake ok
	p1, p2 := net.Pipe()
	wg := &errgroup.Group{}
	gox(wg, func() {
		xhandshake(bg, p1, 1)
	})
	gox(wg, func() {
		xhandshake(bg, p2, 1)
	})
	xwait(wg)
	xclose(p1)
	xclose(p2)

	// version mismatch
	p1, p2 = net.Pipe()
	var err1, err2 error
	wg = &errgroup.Group{}
	gox(wg, func() {
		err1 = handshake(bg, p1, 1)
	})
	gox(wg, func() {
		err2 = handshake(bg, p2, 2)
	})
	xwait(wg)
	xclose(p1)
	xclose(p2)

	err1Want := "pipe - pipe: handshake: protocol version mismatch: peer = 00000002  ; our side = 00000001"
	err2Want := "pipe - pipe: handshake: protocol version mismatch: peer = 00000001  ; our side = 00000002"

	if !(err1 != nil && err1.Error() == err1Want) {
		t.Errorf("handshake ver mismatch: p1: unexpected error:\nhave: %v\nwant: %v", err1, err1Want)
	}
	if !(err2 != nil && err2.Error() == err2Want) {
		t.Errorf("handshake ver mismatch: p2: unexpected error:\nhave: %v\nwant: %v", err2, err2Want)
	}

	// tx & rx problem
	p1, p2 = net.Pipe()
	err1, err2 = nil, nil
	wg = &errgroup.Group{}
	gox(wg, func() {
		err1 = handshake(bg, p1, 1)
	})
	gox(wg, func() {
		xclose(p2)
	})
	xwait(wg)
	xclose(p1)

	err11, ok := err1.(*_HandshakeError)

	if !ok || !(err11.Err == io.ErrClosedPipe /* on Write */ || err11.Err == io.ErrUnexpectedEOF /* on Read */) {
		t.Errorf("handshake peer close: unexpected error: %#v", err1)
	}

	// ctx cancel
	p1, p2 = net.Pipe()
	ctx, cancel := context.WithCancel(bg)
	gox(wg, func() {
		err1 = handshake(ctx, p1, 1)
	})
	tdelay()
	cancel()
	xwait(wg)
	xclose(p1)
	xclose(p2)

	err11, ok = err1.(*_HandshakeError)

	if !ok || !(err11.Err == context.Canceled) {
		t.Errorf("handshake cancel: unexpected error: %#v", err1)
	}

}
