// Copyright (C) 2018-2020  Nexedi SA and Contributors.
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

package zeo
// RPC calls client<->server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/someonegg/gocontainer/rbuf"
	"lab.nexedi.com/kirr/go123/xbytes"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xnet"
	"lab.nexedi.com/kirr/go123/xsync"
)

// we can speak this protocol versions
var protoVersions = []string{
	"3101", // last in ZEO3 series
	"4",    // no longer call load.
	"5",    // current in ZEO5 series.
}


// zLink is ZEO connection between client (local end) and server (remote end).
//
// zLink provides service to make and receive RPC requests.
//
// create zLink via dialZLink or handshake.
type zLink struct {
	link  net.Conn		// underlying network
	rxbuf rbuf.RingBuf	// buffer for reading from link

	// our in-flight calls
	callMu  sync.Mutex
	callTab map[int64]chan msg // msgid -> rxc for that call; nil when closed
	callID  int64              // ID for next call; incremented at every call

	// methods peer can invoke
	// methods are served in parallel
	serveTab map[string]func(context.Context, interface{})interface{}
	// notifications peer can send
	// notifications are invoked in order
	notifyTab map[string]func(interface{})

	serveWg	    sync.WaitGroup  // for serveRecv and serveTab spawned from it
	serveCtx    context.Context // serveTab handlers are called with this ctx
	serveCancel func()          // to cancel serveCtx

	down1     sync.Once
	errClose  error		// error got from .link.Close()

	ver string   // protocol version in use (without "Z" or "M" prefix)
	enc encoding // protocol encoding in use ('Z' or 'M')
}

// (called after handshake)
func (zl *zLink) start() {
	zl.callTab = make(map[int64]chan msg)
	zl.serveCtx, zl.serveCancel = context.WithCancel(context.Background())
	zl.serveWg.Add(1)
	go zl.serveRecv()
}

var errLinkClosed = errors.New("zlink is closed")

// shutdown shuts zlink down and sets error (XXX) which
func (zl *zLink) shutdown(err error) {
	zl.down1.Do(func() {
		if err != nil {
			log.Printf("%s: %s", zl.link.RemoteAddr(), err)
			// XXX what else to do with err?
		}

		zl.serveCancel()
		zl.errClose = zl.link.Close()

		// notify call waiters
		zl.callMu.Lock()
		callTab := zl.callTab
		zl.callTab = nil
		zl.callMu.Unlock()

		for _, rxc := range callTab {
			rxc <- msg{arg: nil} // notify link was closed	XXX ok? or err explicitly?
		}

		// XXX close watcher
	})
}

func (zl *zLink) Close() error {
	zl.shutdown(nil)
	zl.serveWg.Wait() // wait in case shutdown was called from serveRecv
	return zl.errClose
}


// serveRecv handles receives from underlying link and dispatches them to calls
// waiting for results, to notify and serve handlers .
func (zl *zLink) serveRecv() {
	defer zl.serveWg.Done()
	for {
		// receive 1 packet
		pkb, err := zl.recvPkt()
		if err != nil {
			zl.shutdown(err)
			return
		}

		err = zl.serveRecv1(pkb)
		pkb.Free()
		if err != nil {
			zl.shutdown(err)
			return
		}
	}
}

// serveRecv1 handles 1 incoming packet.
func (zl *zLink) serveRecv1(pkb *pktBuf) error {
	// decode packet
	m, err := zl.enc.pktDecode(pkb)
	if err != nil {
		return err
	}

	// message is reply
	if m.method == ".reply" {
		// lookup call by msgid and dispatch result to waiter
		zl.callMu.Lock()
		rxc := zl.callTab[m.msgid]
		if rxc != nil {
			delete(zl.callTab, m.msgid)
		}
		zl.callMu.Unlock()

		if rxc == nil {
			return fmt.Errorf(".%d: unexpected reply", m.msgid)
		}

		rxc <- m
		return nil
	}

	// message is notification
	if m.flags & msgAsync != 0 {
		// notifications go in-order
		f := zl.notifyTab[m.method]
		if f == nil {
			return fmt.Errorf(".%d: unknown notification %q", m.msgid, m.method)
		}

		f(m.arg)
		return nil
	}

	// message is call
	// calls are served in parallel
	f := zl.serveTab[m.method]
	if f == nil {
		// disconnect on call to unknown method
		err = fmt.Errorf("unknown method %q", m.method)
		// XXX error -> exception
		zl.reply(m.msgid, err) // ignore error
		return fmt.Errorf(".%d: %s", m.msgid, err)
	}
	zl.serveWg.Add(1)
	go func() {
		defer zl.serveWg.Done()
		res := f(zl.serveCtx, m.arg)

		// XXX error -> exception

		// send result back
		err := zl.reply(m.msgid, res)
		if err != nil {
			zl.shutdown(err)
		}
	}()

	return nil
}

// tuple represents py tuple.
type tuple []interface{}

// msg represents 1 message.
type msg struct {
	msgid  int64
	flags  msgFlags
	method string
	arg    interface{} // can be e.g. tuple(arg1, arg2, ...)
}

type msgFlags int64
const (
	msgAsync  msgFlags = 1 // message does not need a reply
	msgExcept          = 2 // exception was raised on remote side (ZEO5)
)


// Call makes 1 RPC call to server, waits for reply and returns it.
func (zl *zLink) Call(ctx context.Context, method string, argv ...interface{}) (reply msg, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: call %s: %s", zl.link.RemoteAddr(), method, err)
		}
	}()

	rxc := make(chan msg, 1) // reply will go here

	// register our call
	zl.callMu.Lock()
	if zl.callTab == nil {
		zl.callMu.Unlock()
		return msg{}, errLinkClosed
	}
	callID := zl.callID
	zl.callID++
	zl.callTab[callID] = rxc
	zl.callMu.Unlock()

	// (msgid, async, method, argv)
	pkb := zl.enc.pktEncode(msg{
			msgid:  callID,
			flags:  0,
			method: method,
			arg:    tuple(argv),
	})

	// ok, pkt is ready to go
	err = zl.sendPkt(pkb) // XXX ctx cancel
	if err != nil {
		return msg{}, err
	}

	select {
	case <-ctx.Done():
		return msg{}, ctx.Err()

	case reply = <-rxc:
		if reply.arg == nil {
			// we were woken up because of shutdown
			return msg{}, errLinkClosed
		}
	}

	return reply, nil
}

func (zl *zLink) reply(msgid int64, res interface{}) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: .%d reply: %s", zl.link.RemoteAddr(), msgid, err)
		}
	}()

	pkb := zl.enc.pktEncode(msg{
		msgid:  msgid,
		flags:  msgAsync,
		method: ".reply",
		arg:    res,
	})

	return zl.sendPkt(pkb)
}

/*
// RegisterMethod registers f to be called when remote	XXX
// FIXME -> provide methodTable to dial, so that it is available right from start without any race
func (zl *zLink) RegisterMethod(method string, f func(arg interface{})) {
	// XXX only "async" (without reply)
	// XXX
}
*/

// ---- raw IO ----

// packet = {size(u32), data}
const pktHeaderLen = 4

// pktBuf is buffer with packet data.
//
// alloc via allocPkb and free via pkb.Free.
// similar to skb in Linux.
type pktBuf struct {
	data []byte
}

// Fixup fixes packet length in header according to current packet data.
func (pkb *pktBuf) Fixup() {
	binary.BigEndian.PutUint32(pkb.data, uint32(len(pkb.data) - pktHeaderLen))
}

// Bytes returns whole buffer data including header and payload.
func (pkb *pktBuf) Bytes() []byte {
	return pkb.data
}

// Payload returns payload part of buffer data.
func (pkb *pktBuf) Payload() []byte {
	return pkb.data[pktHeaderLen:]
}

var pkbPool = sync.Pool{New: func() interface{} {
	return &pktBuf{make([]byte, 0, 4096)}
}}

func allocPkb() *pktBuf {
	pkb := pkbPool.Get().(*pktBuf)
	pkb.data = pkb.data[:0]
	pkb.Write([]byte("\x00\x00\x00\x00")) // room for header (= pktHeaderLen)
	return pkb
}

func (pkb *pktBuf) Free() {
	pkbPool.Put(pkb)
}

func (pkb *pktBuf) Write(p []byte) (int, error) {
	pkb.data = append(pkb.data, p...)
	return len(p), nil
}

func (pkb *pktBuf) WriteString(s string) (int, error) {
	pkb.data = append(pkb.data, s...)
	return len(s), nil
}

const dumpio = false


// sendPkt sends 1 raw ZEO packet.
//
// pkb is freed upon return.
func (zl *zLink) sendPkt(pkb *pktBuf) error {
	pkb.Fixup()
	_, err := zl.link.Write(pkb.Bytes())
	if dumpio {
		fmt.Printf("%v > %v: %q\n", zl.link.LocalAddr(), zl.link.RemoteAddr(), pkb.Bytes())
	}
	pkb.Free()
	return err
}

// recvPkt receives 1 raw ZEO packet.
//
// the packet returned contains both header and payload.
// XXX almost dump from NEO.
func (zl *zLink) recvPkt() (*pktBuf, error) {
	pkb := allocPkb()
	data := pkb.data[:cap(pkb.data)]

	n := 0

	// next packet could be already prefetched in part by previous read
	if zl.rxbuf.Len() > 0 {
		δn, _ := zl.rxbuf.Read(data[:pktHeaderLen])
		n += δn
	}

	// first read to read pkt header and hopefully rest of packet in 1 syscall
	if n < pktHeaderLen {
		δn, err := io.ReadAtLeast(zl.link, data[n:], pktHeaderLen - n)
		if err != nil {
			return nil, err
		}
		n += δn
	}

	payloadLen := binary.BigEndian.Uint32(data)
	// XXX check payloadLen for max size
	pktLen := int(pktHeaderLen + payloadLen)

	// resize data if we don't have enough room in it
	data = xbytes.Resize(data, pktLen)
	data = data[:cap(data)]

	// we might have more data already prefetched in rxbuf
	if zl.rxbuf.Len() > 0 {
		δn, _ := zl.rxbuf.Read(data[n:pktLen])
		n += δn
	}

	// read rest of pkt data, if we need to
	if n < pktLen {
		δn, err := io.ReadAtLeast(zl.link, data[n:], pktLen - n)
		if err != nil {
			return nil, err
		}
		n += δn
	}

	// put overread data into rxbuf for next reader
	if n > pktLen {
		zl.rxbuf.Write(data[pktLen:n])
	}

	// fixup data/pkt
	data = data[:n]
	pkb.data = data

	if dumpio {
		fmt.Printf("%v < %v: %q\n", zl.link.LocalAddr(), zl.link.RemoteAddr(), pkb.data)
	}

	return pkb, nil
}


// ---- dial + handshake ----

// dialZLink connects to address on given network, performs ZEO protocol
// handshake and wraps the connection as zLink.
func dialZLink(ctx context.Context, net xnet.Networker, addr string) (*zLink, error) {
	conn, err := net.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}

	return handshake(ctx, conn)
}

// handshake performs ZEO protocol handshake just after raw connection has been
// established in between client and server.
//
// On success raw connection is returned wrapped into zLink.
// On error raw connection is closed.
func handshake(ctx context.Context, conn net.Conn) (_ *zLink, err error) {
	defer xerr.Contextf(&err, "%s: handshake", conn.RemoteAddr())

	// create raw zlink since we need to do the handshake as ZEO message exchange,
	// but don't start serve goroutines yet.
	zl := &zLink{link: conn}

	// ready when/if handshake tx/rx exchange succeeds
	hok := make(chan struct{})

	wg := xsync.NewWorkGroup(ctx)

	// rx/tx handshake packet
	wg.Go(func(ctx context.Context) error {
		// server first announces its preferred protocol
		// it is e.g. "M5", "Z5", "Z4", "Z3101", ...
		//
		// first letter is preferred encoding: 'M' (msgpack), or 'Z' (pickles).
		pkb, err := zl.recvPkt()
		if err != nil {
			return fmt.Errorf("rx: %s", err)
		}

		proto := string(pkb.Payload())
		pkb.Free()
		if !(len(proto) >= 2 && (proto[0] == 'Z' || proto[0] == 'M')) {
			return fmt.Errorf("rx: invalid peer handshake: %q", proto)
		}

		// use wire encoding preferred by server
		enc := encoding(proto[0])

		// extract peer version from protocol string and choose actual
		// version to use as min(peer, mybest)
		ver := proto[1:]
		myBest := protoVersions[len(protoVersions)-1]
		if ver > myBest {
			ver = myBest
		}

		// verify ver is among protocol versions that we support.
		there := false
		for _, weSupport := range protoVersions {
			if ver == weSupport {
				there = true
				break
			}
		}
		if !there {
			return fmt.Errorf("rx: unsupported peer version: %q", proto)
		}

		// version selected - now send it back to server as
		// corresponding handshake reply.
		pkb = allocPkb()
		pkb.WriteString(fmt.Sprintf("%c%s", enc, ver))
		err = zl.sendPkt(pkb)
		if err != nil {
			return fmt.Errorf("tx: %s", err)
		}

		zl.ver = ver
		zl.enc = enc
		close(hok)
		return nil
	})

	wg.Go(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			// either ctx canceled from outside, or it is tx/rx problem.
			// Close connection in any case. If it was not tx/rx
			// problem - we interrupt IO there.
			conn.Close()
			return ctx.Err()

		case <-hok:
			return nil
		}
	})

	err = wg.Wait()
	if err != nil {
		return nil, err
	}

	// handshaked ok
	zl.start()
	return zl, nil
}
