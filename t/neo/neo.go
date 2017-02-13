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

//	// XXX vvv move to ZODB ?
	ZERO_OID     zodb.Oid = 0        // XXX or simply OID{} ?    // XXX -> OID0
	// OID_LEN = 8
	// TID_LEN = 8
)
