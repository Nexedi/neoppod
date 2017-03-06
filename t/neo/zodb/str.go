// TODO copyright / license

// formatting and parsing  for basic zodb types

package zodb

import (
	"fmt"
	"encoding/hex"
	"encoding/binary"

	"lab.nexedi.com/kirr/go123/xstrings"
)

func (tid Tid) String() string {
	// XXX also print "tid:" prefix ?
	return fmt.Sprintf("%016x", uint64(tid))
}

func (oid Oid) String() string {
	// XXX also print "oid:" prefix ?
	return fmt.Sprintf("%016x", uint64(oid))
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
	return xid.XTid.String() + ":" + xid.Oid.String()	// XXX use "·" instead of ":" ?
}


// parseHex64 decode 16-character-wide hex-encoded string into uint64
func parseHex64(subj, s string) (uint64, error) {
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

// ParseTidRange parses string of form "<tidmin>..<tidmax>" into tidMin, tidMax pair
// both <tidmin> and <tidmax> can be empty, in which case defaults 0 and TidMax are returned
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

/*
func (tid Tid) String2() string {
	var b [8+16]byte
	binary.BigEndian.PutUint64(b[:], uint64(tid))
	hex.Encode(b[8:], b[:8])
	//return mem.String(b[:8])
	return string(b[:8])
}
*/
