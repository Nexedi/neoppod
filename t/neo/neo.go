// TODO copyright / license

// Package neo implements distributed object storage for ZODB
// TODO text
package neo

import (
	"./zodb"
)

const (
	//INVALID_UUID UUID = 0
	INVALID_TID  zodb.Tid = 1<<64 - 1            // 0xffffffffffffffff
	INVALID_OID  zodb.Oid = 1<<64 - 1

	// XXX vvv move to ZODB ?
	ZERO_TID     zodb.Tid = 0        // XXX or simply TID{} ?
	TID0         zodb.Tid = ZERO_TID // XXX ^^^ choose 1

	ZERO_OID     zodb.Oid = 0        // XXX or simply OID{} ?    // XXX -> OID0
	// OID_LEN = 8
	// TID_LEN = 8
	MAX_TID      zodb.Tid = 1<<63 - 1		// 0x7fffffffffffffff
	                                        // SQLite does not accept numbers above 2^63-1
						// ZODB also defines maxtid to be max signed int64 since baee84a6 (Jun 7 2016)
	TIDMAX       zodb.Tid = MAX_TID		// XXX ^^^ choose 1
)
