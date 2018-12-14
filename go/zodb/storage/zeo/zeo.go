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

// Package zeo provides simple ZEO client.
package zeo

import (
	"context"
	"encoding/binary"
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
	srv *zLink

	// state we get from server by way of server notifications.
	mu      sync.Mutex
	lastTid zodb.Tid


	url string // we were opened via this
}


func (z *zeo) LastTid(ctx context.Context) (zodb.Tid, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	return z.lastTid, nil
}

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
	xres, err := rpc.call(ctx, oidPack(xid.Oid), tidPack(xid.At+1)) // XXX at2Before
	if err != nil {
		return nil, 0, err
	}

	// (data, serial, next_serial | None)
	res, ok := xres.(pickle.Tuple)
	if !ok || len(res) != 3 {
		return nil, 0, rpc.ereplyf("got %#v; expect 3-tuple", res)
	}

	data, ok1 := res[0].(string)
	serial, ok2 := tidUnpack(res[1])
	// next_serial (res[2]) - just ignore

	if !(ok1 && ok2) {
		return nil, 0, rpc.ereplyf("got (%T, %v, %T); expect (str, tid, .)", res...)
	}

	return &mem.Buf{Data: mem.Bytes(data)}, serial, nil
}

func (z *zeo) Iterate(ctx context.Context, tidMin, tidMax zodb.Tid) zodb.ITxnIterator {
	panic("TODO")
}

func (z *zeo) Watch(ctx context.Context) (zodb.Tid, []zodb.Oid, error) {
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
func (r rpc) excError(exc string, argv []interface{}) error {
	// translate well-known exceptions
	switch exc {
	case "ZODB.POSException.POSKeyError":
		// POSKeyError(oid)
		if len(argv) != 1 {
			return r.ereplyf("poskeyerror: got %#v; expect 1-tuple", argv...)
		}

		oid, ok := oidUnpack(argv[0])
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
	texc, ok := arg.(pickle.Tuple)
	if !ok || len(texc) != 2 {
		return r.ereplyf("except5: got %#v; expect 2-tuple", arg)
	}

	exc, ok1 := texc[0].(string)
	argv, ok2 := texc[1].(pickle.Tuple)
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

	return r.excError(exc, argv)
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

func openByURL(ctx context.Context, u *url.URL, opt *zodb.OpenOptions) (_ zodb.IStorageDriver, err error) {
	url := u.String()
	defer xerr.Contextf(&err, "open %s:", url)

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
		return nil, fmt.Errorf("TODO write mode not implemented")
	}

	zl, err := dialZLink(ctx, net, addr)	// XXX + methodTable
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			zl.Close()
		}
	}()


	z := &zeo{srv: zl, url: url}

	rpc := z.rpc("register")
	xlastTid, err := rpc.call(ctx, storageID, opt.ReadOnly)
	if err != nil {
		return nil, err
	}

	// register returns last_tid in ZEO5 but nothing earlier.
	// if so we have to retrieve last_tid in another RPC.
	if z.srv.ver < "5" {
		rpc = z.rpc("lastTransaction")
		xlastTid, err = rpc.call(ctx)
		if err != nil {
			return nil, err
		}
	}

	lastTid, ok := tidUnpack(xlastTid) // XXX -> xlastTid -> scan
	if !ok {
		return nil, rpc.ereplyf("got %v; expect tid", xlastTid)
	}

	z.lastTid = lastTid


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

	return z, nil
}

func (z *zeo) Close() error {
	return z.srv.Close()
}

func (z *zeo) URL() string {
	return z.url
}


func init() {
	zodb.RegisterDriver("zeo", openByURL)
}


// ---- oid/tid packing ----

// xuint64Unpack tries to decode packed 8-byte string as bigendian uint64
func xuint64Unpack(xv interface{}) (uint64, bool) {
	s, ok := xv.(string)
	if !ok || len(s) != 8 {
		return 0, false
	}

	return binary.BigEndian.Uint64(mem.Bytes(s)), true
}

// xuint64Pack packs v into big-endian 8-byte string
func xuint64Pack(v uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return mem.String(b[:])
}

func tidPack(tid zodb.Tid) string {
	return xuint64Pack(uint64(tid))
}

func oidPack(oid zodb.Oid) string {
	return xuint64Pack(uint64(oid))
}

func tidUnpack(xv interface{}) (zodb.Tid, bool) {
	v, ok := xuint64Unpack(xv)
	return zodb.Tid(v), ok
}

func oidUnpack(xv interface{}) (zodb.Oid, bool) {
	v, ok := xuint64Unpack(xv)
	return zodb.Oid(v), ok
}