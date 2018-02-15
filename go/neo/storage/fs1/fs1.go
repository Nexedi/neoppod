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

// Package fs1 provides NEO storage backend based on ZODB FileStorage.
package fs1

import (
	"context"
	"net/url"

	"lab.nexedi.com/kirr/neo/go/neo/internal/xsha1"
	"lab.nexedi.com/kirr/neo/go/neo/proto"
	"lab.nexedi.com/kirr/neo/go/neo/storage"
	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"
)

type Backend struct {
	// TODO storage layout:
	//	meta/
	//	data/
	//	    1 inbox/	(commit queues)
	//	    2 ? (data.fs)
	//	    3 packed/	(deltified objects)
	//
	// XXX we currently depend on extra functionality FS provides over
	// plain zodb.IStorage (e.g. loading with nextSerial) and even if
	// nextSerial will be gone in the future, we will probably depend on
	// particular layout more and more -> directly work with fs1 & friends.
	//
	// TODO -> abstract into backend interfaces so various backands are
	// possible (e.g. +SQL)
	zstor *fs1.FileStorage // underlying ZODB storage
}

var _ storage.Backend = (*Backend)(nil)

func Open(ctx context.Context, path string) (*Backend, error) {
	zstor, err := fs1.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	return &Backend{zstor: zstor}, nil
}


func (f *Backend) LastTid(ctx context.Context) (zodb.Tid, error) {
	return f.zstor.LastTid(ctx)
}

func (f *Backend) LastOid(ctx context.Context) (zodb.Oid, error) {
	return f.zstor.LastOid(ctx)
}

func (f *Backend) Load(ctx context.Context, xid zodb.Xid) (*proto.AnswerObject, error) {
	// FIXME kill nextSerial support after neo/py cache does not depend on next_serial
	buf, serial, nextSerial, err := f.zstor.Load_XXXWithNextSerialXXX(ctx, xid)
	if err != nil {
		return nil, err
	}

	// no next serial -> None
	if nextSerial == zodb.TidMax {
		nextSerial = proto.INVALID_TID
	}


	return &proto.AnswerObject{
		Oid:	    xid.Oid,
		Serial:     serial,
		NextSerial: nextSerial,

		Compression:	false,
		Data:		buf,
		Checksum:	xsha1.Sum(buf.Data),	// XXX computing every time

		// XXX .DataSerial
	}, nil
}


// ---- open by URL ----

func openURL(ctx context.Context, u *url.URL) (storage.Backend, error) {
	// TODO handle query
	// XXX u.Path is not always raw path - recheck and fix
	path := u.Host + u.Path
	return Open(ctx, path)
}

func init() {
	storage.RegisterBackend("fs1", openURL)
}
