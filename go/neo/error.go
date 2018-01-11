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

package neo
// error related utilities

import (
	"strings"

	"lab.nexedi.com/kirr/neo/go/zodb"
)


// XXX place=?	-> methods of Error

// ErrEncode translates an error into Error packet.
// XXX more text describing relation with zodb errors
func ErrEncode(err error) *Error {
	switch err := err.(type) {
	case *Error:
		return err
	case *zodb.NoDataError:
		// XXX abusing message for oid, deletedAt
		return &Error{
			Code:    OID_NOT_FOUND,
			Message: err.Oid.String() + "," + err.DeletedAt.String(),
		}

	case *zodb.NoObjectError:
		// XXX abusing message for oid
		return &Error{Code: OID_DOES_NOT_EXIST, Message: err.Oid.String()}

	default:
		return &Error{Code: NOT_READY /* XXX how to report 503? was BROKEN_NODE */, Message: err.Error()}
	}

}

// ErrDecode decodes error from Error packet.
// XXX more text describing relation with zodb errors
func ErrDecode(e *Error) error {
	switch e.Code {
	case OID_NOT_FOUND:
		// XXX abusing message for oid, deletedAt
		argv := strings.Split(e.Message, ",")
		if len(argv) != 2 {
			break
		}
		oid, err0 := zodb.ParseOid(argv[0])
		del, err1 := zodb.ParseTid(argv[1])
		if !(err0 == nil && err1 == nil) {
			break
		}
		return &zodb.NoDataError{Oid: oid, DeletedAt: del}

	case OID_DOES_NOT_EXIST:
		oid, err := zodb.ParseOid(e.Message)	// XXX abusing message for oid
		if err == nil {
			return &zodb.NoObjectError{oid}
		}
	}

	return e
}
