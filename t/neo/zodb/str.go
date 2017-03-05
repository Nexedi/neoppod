// TODO copyright / license

// formatting and scanning for basic zodb types

package zodb

import (
	"fmt"
	"encoding/hex"
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


func ParseTid(s string) (Tid, error) {
	// -> scanf("%016x")
	var b[8]byte
	if hex.DecodedLdn(len(s)) != 8 {
		return 0, fmt.Errorf("hex64parse: %q invalid len", s)	// XXX
	}
	_, err = hex.Decode(b[:], mem.Bytes(s))
	if err != nil {
		...
	}
}
