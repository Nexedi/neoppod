package neo
// TODO move me properly -> connection.go

import (
	"fmt"

	"../zodb"

	"lab.nexedi.com/kirr/go123/xerr"
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

// ------------------------------------------
// XXX place=?

// IdentifyWith identifies local node with remote peer
// it also verifies peer's node type to what caller expects
func IdentifyWith(expectPeerType NodeType, link *NodeLink, myInfo NodeInfo, clusterName string) (accept *AcceptIdentification, err error) {
	defer xerr.Contextf(&err, "%s: request identification", link)

	conn, err := link.NewConn()
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := conn.Close()
		if err == nil && err2 != nil {
			err = err2
		}
	}()

	accept = &AcceptIdentification{}
	err = conn.Ask(&RequestIdentification{
		NodeType:	 myInfo.NodeType,
		NodeUUID:	 myInfo.NodeUUID,
		Address:	 myInfo.Address,
		ClusterName:	 clusterName,
		IdTimestamp:	 myInfo.IdTimestamp,	// XXX ok?
	}, accept)

	if err != nil {
		return nil, err // XXX err ctx ?
	}

	if accept.NodeType != expectPeerType {
		return nil, fmt.Errorf("accepted, but peer is not %v (identifies as %v)", expectPeerType, accept.NodeType)
	}

	return accept, nil
}
