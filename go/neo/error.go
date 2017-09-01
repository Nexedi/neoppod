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
	"lab.nexedi.com/kirr/neo/go/zodb"
)


// XXX place=?	-> methods of Error

// errEncode translates an error into Error packet
// XXX more text describing relation with zodb errors
func ErrEncode(err error) *Error {
	switch err := err.(type) {
	case *Error:
		return err
	case *zodb.ErrXidMissing:
		// XXX abusing message for xid
		return &Error{Code: OID_NOT_FOUND, Message: err.Xid.String()}

	default:
		return &Error{Code: NOT_READY /* XXX how to report 503? was BROKEN_NODE */, Message: err.Error()}
	}

}

// errDecode decodes error from Error packet
// XXX more text describing relation with zodb errors
func ErrDecode(e *Error) error {
	switch e.Code {
	case OID_NOT_FOUND:
		xid, err := zodb.ParseXid(e.Message)	// XXX abusing message for xid
		if err == nil {
			return &zodb.ErrXidMissing{xid}
		}
	}

	return e
}
