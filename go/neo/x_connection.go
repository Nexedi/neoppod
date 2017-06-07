package neo
// TODO move me properly -> connection.go

import (
	"fmt"
	"reflect"

	"../zodb"

	"lab.nexedi.com/kirr/go123/xerr"
)

// RecvAndDecode receives packet from conn and decodes it
func RecvAndDecode(conn *Conn) (Pkt, error) {
	pkt, err := conn.Recv()
	if err != nil {
		return nil, err
	}

	// decode packet
	pkth := pkt.Header()
	msgCode := ntoh16(pkth.MsgCode)
	msgType := pktTypeRegistry[msgCode]
	if msgType == nil {
		err = fmt.Errorf("invalid msgCode (%d)", msgCode)
		// XXX -> ProtoError ?
		return nil, &ConnError{Conn: conn, Op: "decode", Err: err}
	}

	// TODO use free-list for decoded packets + when possible decode in-place
	pktObj := reflect.New(msgType).Interface().(Pkt)
	_, err = pktObj.NEOPktDecode(pkt.Payload())
	if err != nil {
		// XXX -> ProtoError ?
		return nil, &ConnError{Conn: conn, Op: "decode", Err: err}
	}

	return pktObj, nil
}

// EncodeAndSend encodes pkt and sends it to conn
func EncodeAndSend(conn *Conn, pkt Pkt) error {
	l := pkt.NEOPktEncodedLen()
	buf := PktBuf{make([]byte, PktHeadLen + l)}	// XXX -> freelist

	h := buf.Header()
	// h.ConnId will be set by conn.Send
	h.MsgCode = hton16(pkt.NEOPktMsgCode())
	h.MsgLen = hton32(uint32(l))	// XXX casting: think again

	pkt.NEOPktEncode(buf.Payload())

	return conn.Send(&buf)	// XXX why pointer?
}

// Ask does simple request/response protocol exchange
// It expects the answer to be exactly of resp type and errors otherwise
func Ask(conn *Conn, req Pkt, resp Pkt) error {
	err := EncodeAndSend(conn, req)
	if err != nil {
		return err
	}

	err = Expect(conn, resp)
	return err
}


// ProtoError is returned when there waa a protocol error, like receiving
// unexpected packet or packet with wrong header
// XXX -> ConnError{Op: "decode"} ?
type ProtoError struct {
	Conn *Conn
	Err  error
}

func (e *ProtoError) Error() string {
	return fmt.Sprintf("%v: %v", e.Conn, e.Err)
}

// Expect receives 1 packet and expects it to be exactly of msg type
// XXX naming  (-> Recv1 ?)
func Expect(conn *Conn, msg Pkt) (err error) {
	pkt, err := conn.Recv()
	if err != nil {
		return err
	}

	// received ok. Now it is all decoding

	// XXX dup wrt RecvAndDecode
	pkth := pkt.Header()
	msgCode := ntoh16(pkth.MsgCode)

	if msgCode != msg.NEOPktMsgCode() {
		// unexpected Error response
		if msgCode == (&Error{}).NEOPktMsgCode() {
			errResp := Error{}
			_, err = errResp.NEOPktDecode(pkt.Payload())
			if err != nil {
				return &ProtoError{conn, err}
			}

			// FIXME clarify error decoding logic:
			// - in some cases Error is one of "expected" answers (e.g. Ask(GetObject))
			// - in other cases Error is completely not expected
			//   (e.g. getting 1st packet on connection)
			return ErrDecode(&errResp) // XXX err ctx vs ^^^ errcontextf ?
		}

		msgType := pktTypeRegistry[msgCode]
		if msgType == nil {
			return &ProtoError{conn, fmt.Errorf("invalid msgCode (%d)", msgCode)}
		}

		return &ProtoError{conn, fmt.Errorf("unexpected packet: %v", msgType)}
	}

	_, err = msg.NEOPktDecode(pkt.Payload())
	if err != nil {
		return &ProtoError{conn, err}
	}

	return nil
}


// ------------------------------------------
// XXX place=?

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
	err = Ask(conn, &RequestIdentification{
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
