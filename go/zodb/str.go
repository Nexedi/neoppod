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

package zodb
// formatting and parsing for basic zodb types

import (
	"fmt"
	"encoding/hex"
	"encoding/binary"

	"lab.nexedi.com/kirr/go123/xfmt"
	"lab.nexedi.com/kirr/go123/xstrings"
)

func (tid Tid) String() string {
	return string(tid.XFmtString(nil))
}

func (oid Oid) String() string {
	return string(oid.XFmtString(nil))
}

func (tid Tid) XFmtString(b []byte) []byte {
	// XXX also print "tid:" prefix ?
	return xfmt.AppendHex016(b, uint64(tid))
}

func (oid Oid) XFmtString(b []byte) []byte {
	// XXX also print "oid:" prefix ?
	return xfmt.AppendHex016(b, uint64(oid))
}

// XXX move me out of here
// bint converts bool to int with true => 1; false => 0
func bint(b bool) int {
	if b {
		return 1
	} else {
		return 0
	}
}

func (xtid XTid) String() string {
	// XXX also print "tid:" prefix ?
	return fmt.Sprintf("%c%v", "=<"[bint(xtid.TidBefore)], xtid.Tid)
}

func (xid Xid) String() string {
	return xid.XTid.String() + ":" + xid.Oid.String()
}

/* TODO reenable?
func (xtid XTid) XFmtString(b []byte) []byte {
	b .C("=<"[bint(xtid.TidBefore)]) .V(xtid.Tid)
}

func (xid Xid) XFmtString(b xfmt.Buffer) xfmt.Buffer {
	b .V(xid.XTid) .C(':') .V(xid.Oid)
}
*/


// parseHex64 decodes 16-character-wide hex-encoded string into uint64
func parseHex64(subj, s string) (uint64, error) {
	// XXX -> xfmt ?
	// XXX like scanf("%016x") but scanf implicitly skips spaces without giving control to caller and is slower
	var b[8]byte
	if len(s) != 16 {
		return 0, fmt.Errorf("%s %q invalid", subj, s)
	}
	_, err := hex.Decode(b[:], []byte(s))
	if err != nil {
		return 0, fmt.Errorf("%s %q invalid", subj, s)
	}

	return binary.BigEndian.Uint64(b[:]), nil
}

func ParseTid(s string) (Tid, error) {
	x, err := parseHex64("tid", s)
	return Tid(x), err
}

func ParseOid(s string) (Oid, error) {
	x, err := parseHex64("oid", s)
	return Oid(x), err
}

func ParseXTid(s string) (XTid, error) {
	if len(s) < 1 {
		goto Error
	}

	{
		var tidBefore bool
		switch s[0] {
		case '<':
			tidBefore = true
		case '=':
			tidBefore = false
		default:
			goto Error
		}

		tid, err := ParseTid(s[1:])
		if err != nil {
			goto Error
		}

		return XTid{tid, tidBefore}, nil
	}


Error:
	return XTid{}, fmt.Errorf("xtid %q invalid", s)
}

func ParseXid(s string) (Xid, error) {
	xtids, oids, err := xstrings.Split2(s, ":")
	if err != nil {
		goto Error
	}

	{
		xtid, err1 := ParseXTid(xtids)
		oid, err2 := ParseOid(oids)
		if err1 != nil || err2 != nil {
			goto Error
		}

		return Xid{xtid, oid}, nil
	}

Error:
	return Xid{}, fmt.Errorf("xid %q invalid", s)
}

// ParseTidRange parses string of form "<tidmin>..<tidmax>" into tidMin, tidMax pair.
//
// Both <tidmin> and <tidmax> can be empty, in which case defaults 0 and TidMax are used.
//
// XXX also check tidMin < tidMax here? or allow reverse ranges ?
func ParseTidRange(s string) (tidMin, tidMax Tid, err error) {
	s1, s2, err := xstrings.Split2(s, "..")
	if err != nil {
		goto Error
	}

	tidMin = 0
	tidMax = TidMax

	if s1 != "" {
		tidMin, err = ParseTid(s1)
		if err != nil {
			goto Error
		}
	}

	if s2 != "" {
		tidMax, err = ParseTid(s2)
		if err != nil {
			goto Error
		}
	}

	return tidMin, tidMax, nil

Error:
	return 0, 0, fmt.Errorf("tid range %q invalid", s)
}
