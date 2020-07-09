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
	link *zLink

	// driver client <- watcher: database commits | errors.
	watchq  chan<- zodb.Event
	head    zodb.Tid            // last invalidation received from server
	at0Mu   sync.Mutex
	at0     zodb.Tid            // at0 obtained when initially connecting to server
	eventq0 []*zodb.EventCommit // buffer for initial messeages, until .at0 is initialized

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

	head, ok := z.link.enc.asTid(xhead)
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
	enc := z.link.enc
	xres, err := rpc.call(ctx, enc.oidPack(xid.Oid), enc.tidPack(xid.At+1)) // XXX at2Before
	if err != nil {
		return nil, 0, err
	}

	// (data, serial, next_serial | None)
	res, ok := enc.asTuple(xres)
	if !ok || len(res) != 3 {
		return nil, 0, rpc.ereplyf("got %#v; expect 3-tuple", xres)
	}

	data, ok1 := enc.asBytes(res[0])
	serial, ok2 := enc.asTid(res[1])
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


// invalidateTransaction receives invalidations from server
func (z *zeo) invalidateTransaction(arg interface{}) (err error) {
	defer xerr.Context(&err, "invalidateTransaction")

	enc := z.link.enc
	t, ok := enc.asTuple(arg)
	if !ok || len(t) != 2 {
		return fmt.Errorf("got %#v; expect 2-tuple", arg)
	}

	// (tid, oidv)
	tid, ok1 := enc.asTid(t[0])
	xoidt, ok2 := enc.asTuple(t[1])
	if !(ok1 && ok2) {
		return fmt.Errorf("got (%T, %T); expect (tid, []oid)", t...)
	}
	oidv := []zodb.Oid{}
	for _, xoid := range xoidt {
		oid, ok := enc.asOid(xoid)
		if !ok {
			return fmt.Errorf("non-oid %#v in oidv", xoid)
		}
		oidv = append(oidv, oid)
	}

	if tid <= z.head {
		return fmt.Errorf("bad invalidation from server: tid not ↑: %s -> %s", z.head, tid)
	}
	z.head = tid

	if z.watchq == nil {
		return nil
	}

	// invalidation event received and we have to send it to .watchq
	event := &zodb.EventCommit{Tid: tid, Changev: oidv}

	z.at0Mu.Lock()
	defer z.at0Mu.Unlock()

	// queue initial events until .at0 is initalized after register
	if z.at0 == 0 {
		z.eventq0 = append(z.eventq0, event)
		return nil
	}

	// at0 is initialized - ok to send current event if it goes > at0
	if tid > z.at0 {
		z.watchq <- event
	}
	return nil
}

// ----------------------------------------

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
	return rpc{zl: z.link, method: method}
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

		oid, ok := r.zl.enc.asOid(argv[0])
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
	enc := r.zl.enc
	// ('type', (arg1, arg2, arg3, ...))
	texc, ok := enc.asTuple(arg)
	if !ok || len(texc) != 2 {
		return r.ereplyf("except5: got %#v; expect 2-tuple", arg)
	}

	exc, ok1 := enc.asString(texc[0])
	argv, ok2 := enc.asTuple(texc[1])
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

	z := &zeo{watchq: opt.Watchq, url: url}

	//zl, err := dialZLink(ctx, net, addr)	// XXX + methodTable {invalidateTransaction tid, oidv} -> ...
	zl, err := dialZLink(ctx, net, addr,
	/*
		// notifyTab
		map[string]func(interface{})error {
			"invalidateTransaction": z.invalidateTransaction,
		},
		// serveTab
		nil,
	*/
	)
	if err != nil {
		return nil, zodb.InvalidTid, err
	}

	defer func() {
		if err != nil {
			zl.Close()
		}
	}()


	z.link = zl

	rpc := z.rpc("register")
	xlastTid, err := rpc.call(ctx, storageID, opt.ReadOnly)
	if err != nil {
		return nil, zodb.InvalidTid, err
	}

	// register returns last_tid in ZEO5 but nothing earlier.
	// if so we have to retrieve last_tid in another RPC.
	if z.link.ver < "5" {
		rpc = z.rpc("lastTransaction")
		xlastTid, err = rpc.call(ctx)
		if err != nil {
			return nil, zodb.InvalidTid, err
		}
	}

	lastTid, ok := zl.enc.asTid(xlastTid)
	if !ok {
		return nil, zodb.InvalidTid, rpc.ereplyf("got %v; expect tid", xlastTid)
	}

	// since we read lastTid, at least with ZEO < 5, in separate RPC
	// call, there is a chance, that by the time when lastTid was read, some
	// new transactions were committed. This way lastTid will be > than
	// some first transactions received by watcher via
	// "invalidateTransaction" server notification.
	//
	// filter-out first < at0 messages for this reason.
	z.at0Mu.Lock()
	z.at0 = lastTid
	if z.watchq != nil {
		for _, e := range z.eventq0 {
			if e.Tid > lastTid {
				z.watchq <- e
			}
		}
	}
	z.eventq0 = nil
	z.at0Mu.Unlock()



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

	return z, z.at0, nil
}

func (z *zeo) Close() error {
	err := z.link.Close()
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
