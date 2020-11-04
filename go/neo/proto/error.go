// Copyright (C) 2017  Nexedi SA and Contributors.
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

package proto
// error related utilities

import (
	"strings"

	"lab.nexedi.com/kirr/neo/go/zodb"
)

// XXX should be not in proto/ ?

// ZODBErrEncode translates a ZODB error into Error packet.
//
// ZODB errors (e.g. zodb.NoDataError) are specifically encoded, so that on
// receiver side they can be recreated with ErrDecode. If err is zodb.OpError,
// only its inner cause is encoded.
//
// If err is not ZODB error -> it is incoded as "503".
func ZODBErrEncode(err error) *Error {
	e, ok := err.(*zodb.OpError)
	if ok {
		err = e.Err
	}

	switch err := err.(type) {
	case *zodb.NoDataError:
		// message: oid(,deletedAt)?
		return &Error{
			Code:    OID_NOT_FOUND,
			Message: err.Oid.String() + "," + err.DeletedAt.String(),
		}

	case *zodb.NoObjectError:
		// message: oid
		return &Error{Code: OID_DOES_NOT_EXIST, Message: err.Oid.String()}

	default:
		return &Error{Code: NOT_READY /* XXX how to report 503? was BROKEN_NODE */, Message: err.Error()}
	}

}

// ZODBErrDecode decodes an error from Error packet.
//
// If it was ZODB error - it is decoded from the packet and returned.
// Otherwise e is returned as is.
func ZODBErrDecode(e *Error) error {
	switch e.Code {
	case OID_NOT_FOUND:
		// message: oid(,deletedAt)?
		zerr := &zodb.NoDataError{}
		argv := strings.Split(e.Message, ",")
		var err error
		zerr.Oid, err = zodb.ParseOid(argv[0])
		if err != nil {
			break
		}
		if len(argv) >= 2 {
			zerr.DeletedAt, err = zodb.ParseTid(argv[1])
			if err != nil {
				break
			}
		}
		return zerr

	case OID_DOES_NOT_EXIST:
		// message: oid
		oid, err := zodb.ParseOid(e.Message)
		if err == nil {
			return &zodb.NoObjectError{oid}
		}
	}

	return e
}
