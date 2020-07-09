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

// Package zeo provides simple ZEO client.
package zeo

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"

	pickle "github.com/kisielk/og-rek"

	"lab.nexedi.com/kirr/go123/mem"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xnet"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

type zeo struct {
	srv *zLink	// XXX rename -> link?

	// state we get from server by way of server notifications.
	mu      sync.Mutex
	lastTid zodb.Tid

	// driver client <- watcher: database commits | errors.
	watchq chan<- zodb.Event // FIXME stub

	url string // we were opened via this
}


// Sync implements zodb.IStorageDriver.
func (z *zeo) Sync(ctx context.Context) (head zodb.Tid, err error) {
	defer func() {
		if err != nil {
			err = &zodb.OpError{URL: z.URL(), Op: "sync", Args: nil, Err: err}
		}
	}()

	rpc := z.rpc("lastTransaction")
	xhead, err := rpc.call(ctx)
	if err != nil {
		return zodb.InvalidTid, err
	}

	head, ok := z.srv.tidUnpack(xhead)
	if !ok {
		return zodb.InvalidTid, rpc.ereplyf("got %v; expect tid", xhead)
	}

	return head, nil
}

// Load implements zodb.IStorageDriver.
func (z *zeo) Load(ctx context.Context, xid zodb.Xid) (*mem.Buf, zodb.Tid, error) {
	// defer func() ...
	buf, serial, err := z._Load(ctx, xid)
	if err != nil {
		err = &zodb.OpError{URL: z.URL(), Op: "load", Args: xid, Err: err}
	}
	return buf, serial, err
}

func (z *zeo) _Load(ctx context.Context, xid zodb.Xid) (*mem.Buf, zodb.Tid, error) {
	rpc := z.rpc("loadBefore")
	xres, err := rpc.call(ctx, z.srv.oidPack(xid.Oid), z.srv.tidPack(xid.At+1)) // XXX at2Before
	if err != nil {
		return nil, 0, err
	}

	// (data, serial, next_serial | None)
	res, ok := z.srv.asTuple(xres)
	if !ok || len(res) != 3 {
		return nil, 0, rpc.ereplyf("got %#v; expect 3-tuple", xres)
	}

	data, ok1 := z.srv.asBytes(res[0])
	serial, ok2 := z.srv.tidUnpack(res[1])
	// next_serial (res[2]) - just ignore

	if !(ok1 && ok2) {
		return nil, 0, rpc.ereplyf("got (%T, %v, %T); expect (str, tid, .)", res...)
	}

	return &mem.Buf{Data: data}, serial, nil
}

// Iterates implements zodb.IStorageDriver.
func (z *zeo) Iterate(ctx context.Context, tidMin, tidMax zodb.Tid) zodb.ITxnIterator {
	panic("TODO")
}


// errorUnexpectedReply is returned by zLink.Call callers when reply was
// received successfully, but is not what the caller expected.
type errorUnexpectedReply struct {
	Addr   string
	Method string
	Err    error
}

func (e *errorUnexpectedReply) Error() string {
	return fmt.Sprintf("%s: call %s: unexpected reply: %s", e.Addr, e.Method, e.Err)
}

func ereplyf(addr, method, format string, argv ...interface{}) *errorUnexpectedReply {
	return &errorUnexpectedReply{
		Addr:   addr,
		Method: method,
		Err:    fmt.Errorf(format, argv...),
	}
}

// rpc returns rpc object handy to make calls/create errors
func (z *zeo) rpc(method string) rpc {
	return rpc{zl: z.srv, method: method}
}

type rpc struct {
	zl     *zLink
	method string
}

// rpcExcept represents generic exception
type rpcExcept struct {
	exc  string
	argv []interface{}
}

func (r *rpcExcept) Error() string {
	return fmt.Sprintf("exception: %s %q", r.exc, r.argv)
}

func (r rpc) call(ctx context.Context, argv ...interface{}) (interface{}, error) {
	reply, err := r.zl.Call(ctx, r.method, argv...)
	if err != nil {
		return nil, err
	}

	if r.zl.ver >= "5" {
		// in ZEO5 exceptions are marked via flag
		if reply.flags & msgExcept != 0 {
			return nil, r.zeo5Error(reply.arg)
		}
	} else {
		// in ZEO < 5 exceptions are represented by returning
		// (exc_class, exc_inst) - check it
		err = r.zeo4Error(reply.arg)
		if err != nil {
			return nil, err
		}
	}

	// it is not an exception
	return reply.arg, nil
}

// excError returns error corresponding to an exception.
//
// well-known exceptions are mapped to corresponding well-known errors - e.g.
// POSKeyError -> zodb.NoObjectError, and rest are returned wrapper into rpcExcept.
func (r rpc) excError(exc string, argv tuple) error {
	// translate well-known exceptions
	switch exc {
	case "ZODB.POSException.POSKeyError":
		// POSKeyError(oid)
		if len(argv) != 1 {
			return r.ereplyf("poskeyerror: got %#v; expect 1-tuple", argv...)
		}

		oid, ok := r.zl.oidUnpack(argv[0])
		if !ok {
			return r.ereplyf("poskeyerror: got (%v); expect (oid)", argv[0])
		}

		// XXX POSKeyError does not allow to distinguish whether it is
		// no object at all or object exists and its data was not found
		// for tid_before. IOW we cannot translate to zodb.NoDataError
		return &zodb.NoObjectError{Oid: oid}
	}

	return &rpcExcept{exc, argv}
}

// zeo5Error decodes arg of reply with msgExcept flag set and returns
// corresponding error.
func (r rpc) zeo5Error(arg interface{}) error {
	// ('type', (arg1, arg2, arg3, ...))
	texc, ok := r.zl.asTuple(arg)
	if !ok || len(texc) != 2 {
		return r.ereplyf("except5: got %#v; expect 2-tuple", arg)
	}

	exc, ok1 := r.zl.asString(texc[0])
	argv, ok2 := r.zl.asTuple(texc[1])
	if !(ok1 && ok2) {
		return r.ereplyf("except5: got (%T, %T); expect (str, tuple)", texc...)
	}

	return r.excError(exc, argv)
}

// zeo4Error checks whether arg corresponds to exceptional reply, and if
// yes, decodes it into corresponding error.
//
// nil is returned if arg does not represent an exception.
func (r rpc) zeo4Error(arg interface{}) error {
	// XXX check r.zl.encoding == 'Z' before using pickles?

	// (exc_class, exc_inst), e.g.
	// ogórek.Tuple{
	//         ogórek.Class{Module:"ZODB.POSException", Name:"POSKeyError"},
	//         ogórek.Call{
	//                 Callable: ogórek.Class{Module:"ZODB.POSException", Name:"_recon"},
	//                 Args:     ogórek.Tuple{
	//                         ogórek.Class{Module:"ZODB.POSException", Name:"POSKeyError"},
	//                         map[interface {}]interface {}{
	//                                 "args":ogórek.Tuple{"\x00\x00\x00\x00\x00\x00\bP"}
	//                         }
	//                 }
	//         }
	// }
	targ, ok := arg.(pickle.Tuple)
	if !ok || len(targ) != 2 {
		return nil
	}

	klass, ok := targ[0].(pickle.Class)
	if !ok || !isPyExceptClass(klass) {
		return nil
	}
	exc  := klass.Module + "." + klass.Name

	// it is exception
	call, ok := targ[1].(pickle.Call)
	if !ok {
		// not a call - the best we can do is to guess
		return r.ereplyf("except4: %s: inst %#v; expect call", exc, targ[1:])
	}

	exc  = call.Callable.Module + "." + call.Callable.Name
	argv := call.Args
	if exc == "ZODB.POSException._recon" {
		// args: (class, state)
		if len(argv) != 2 {
			return r.ereplyf("except4: %s: got %#v; expect 2-tuple", exc, argv)
		}

		klass, ok1 := argv[0].(pickle.Class)
		state, ok2 := argv[1].(map[interface{}]interface{})
		if !(ok1 && ok2) {
			return r.ereplyf("except4: %s: got (%T, %T); expect (class, dict)", exc, argv[0], argv[1])
		}

		args, ok := state["args"].(pickle.Tuple)
		if !ok {
			return r.ereplyf("except4: %s: state.args = %#v; expect tuple", exc, state["args"])
		}

		exc  = klass.Module + "." + klass.Name
		argv = args
	}

	return r.excError(exc, tuple(argv)) // XXX don't cast?
}

// isPyExceptClass returns whether klass represents python exception
func isPyExceptClass(klass pickle.Class) bool {
	// XXX this is approximation
	if strings.HasSuffix(klass.Name, "Error") {
		return true
	}

	return false
}

func (r rpc) ereplyf(format string, argv ...interface{}) *errorUnexpectedReply {
	return ereplyf(r.zl.link.RemoteAddr().String(), r.method, format, argv...)
}


// ---- open ----

func openByURL(ctx context.Context, u *url.URL, opt *zodb.DriverOptions) (_ zodb.IStorageDriver, at0 zodb.Tid, err error) {
	url := u.String()
	defer xerr.Contextf(&err, "open %s", url)

	// zeo://host:port/path?storage=...&...
	var net  xnet.Networker
	var addr string

	if u.Host != "" {
		net  = xnet.NetPlain("tcp")
		addr = u.Host
	} else {
		net  = xnet.NetPlain("unix")
		addr = u.Path
	}

	storageID := "1"

	q := u.Query()
	if s := q.Get("storage"); s != "" {
		storageID = s
	}

	if !opt.ReadOnly {
		return nil, zodb.InvalidTid, fmt.Errorf("TODO write mode not implemented")
	}

	// FIXME handle opt.Watchq
	// for now we pretend as if the database is not changing.
	// TODO watcher(when implementing): filter-out first < at0 messages.
	if opt.Watchq != nil {
		log.Print("zeo: FIXME: watchq support not implemented - there " +
			  "won't be notifications about database changes")
	}

	zl, err := dialZLink(ctx, net, addr)	// XXX + methodTable {invalidateTransaction tid, oidv} -> ...
	if err != nil {
		return nil, zodb.InvalidTid, err
	}

	defer func() {
		if err != nil {
			zl.Close()
		}
	}()


	z := &zeo{srv: zl, watchq: opt.Watchq, url: url}

	rpc := z.rpc("register")
	xlastTid, err := rpc.call(ctx, storageID, opt.ReadOnly)
	if err != nil {
		return nil, zodb.InvalidTid, err
	}

	// register returns last_tid in ZEO5 but nothing earlier.
	// if so we have to retrieve last_tid in another RPC.
	if z.srv.ver < "5" {
		rpc = z.rpc("lastTransaction")
		xlastTid, err = rpc.call(ctx)
		if err != nil {
			return nil, zodb.InvalidTid, err
		}
	}

	lastTid, ok := zl.tidUnpack(xlastTid) // XXX -> xlastTid -> scan
	if !ok {
		return nil, zodb.InvalidTid, rpc.ereplyf("got %v; expect tid", xlastTid)
	}

	z.lastTid = lastTid

	// XXX since we read lastTid, at least with ZEO < 5, in separate RPC
	// call, there is a chance, that by the time when lastTid was read some
	// new transactions were committed. This way lastTid will be > than
	// some first transactions received by watcher via
	// "invalidateTransaction" server notification.
	at0 = lastTid


	//call('get_info') -> {}str->str, ex	// XXX can be omitted
/*
 {'interfaces': (('ZODB.interfaces', 'IStorageRestoreable'),
   ('ZODB.interfaces', 'IStorageIteration'),
   ('ZODB.interfaces', 'IStorageUndoable'),
   ('ZODB.interfaces', 'IStorageCurrentRecordIteration'),
   ('ZODB.interfaces', 'IExternalGC'),
   ('ZODB.interfaces', 'IStorage'),
   ('zope.interface', 'Interface')),
  'length': 2128,
  'name': '/home/kirr/src/neo/src/lab.nexedi.com/kirr/neo/go/neo/t/var/wczblk1-8/fs1/data.fs',
  'size': 8630075,
  'supportsUndo': True,
  'supports_record_iternext': True})
*/

	return z, at0, nil
}

func (z *zeo) Close() error {
	err := z.srv.Close()
	if z.watchq != nil {
		close(z.watchq)
	}
	return err
}

func (z *zeo) URL() string {
	return z.url
}


func init() {
	zodb.RegisterDriver("zeo", openByURL)
}
