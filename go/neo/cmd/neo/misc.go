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

package main
// routines common to several subcommands

import (
	"context"
	"encoding/binary"
	stdnet "net"
	"net/http"
	"io"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/soheilhy/cmux"

	"lab.nexedi.com/kirr/go123/xnet"
	"lab.nexedi.com/kirr/neo/go/xcommon/log"


	_ "net/http/pprof"
)

// neoMatch tells whether incoming stream starts like a NEO protocol handshake word.
func neoMatch(r io.Reader) bool {
	var b [4]byte
	n, _ := io.ReadFull(r, b[:])
	if n < 4 {
		return false
	}

	version := binary.BigEndian.Uint32(b[:])
	return (version < 0xff) // so it looks like 00 00 00 v
}

// listenAndServe runs service on laddr.
//
// It starts listening, multiplexes incoming connection to NEO and HTTP
// protocols, passes NEO connections to service and passes HTTP connection to
// default HTTP mux.
//
// default HTTP mux can be assumed to contain /debug/pprof and the like.
func listenAndServe(ctx context.Context, net xnet.Networker, laddr string, serve func(ctx context.Context, l stdnet.Listener) error) error {
	l, err := net.Listen(laddr)
	if err != nil {
		return err
	}

	// XXX who closes l?

	log.Infof(ctx, "listening at %s ...", l.Addr())
	log.Flush() // XXX ok?

	mux := cmux.New(l)
	neoL  := mux.Match(neoMatch)
	httpL := mux.Match(cmux.HTTP1(), cmux.HTTP2()) // XXX verify http2 works
	miscL := mux.Match(cmux.Any())

	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		// XXX shutdown serve on ctx cancel
		return mux.Serve()
	})

	wg.Go(func() error {
		return serve(ctx, neoL)
	})

	wg.Go(func() error {
		// XXX shutdown http on ctx cancel
		return http.Serve(httpL, nil)
	})

	wg.Go(func() error {
		// XXX shutdown on ctx cancel
		for {
			conn, err := miscL.Accept()
			if err != nil {
				return err
			}

			// got something unexpected - grab the header (which we
			// already have read), log it and reject the
			// connection.
			b := make([]byte, 1024)
			// must not block as some data is already there in cmux buffer
			n, _ := conn.Read(b)
			subj := fmt.Sprintf("strange connection from %s:", conn.RemoteAddr())
			serr := "peer sent nothing"
			if n > 0 {
				serr = fmt.Sprintf("peer sent %q", b[:n])
			}
			log.Infof(ctx, "%s: %s", subj, serr)

			conn.Close()	// XXX lclose
		}
	})



	err = wg.Wait()
	return err
}
