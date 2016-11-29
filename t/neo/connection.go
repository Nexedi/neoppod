// TODO copyright / license

// TODO text

package neo

import (
	"net"
)

// NodeLink is a node-node connection in NEO
// A node-node connection represents bidirectional symmetrical communication
// link in between 2 NEO nodes. The link provides service for packets
// exchange and for multiplexing several higher-level communication channels on
// top of node-node link.
//
// All packets are classified to be of one of the following kind:
// - notify:	a packet is sent without expecting any reply
// - ask:	a packet is sent and reply is expected
// - answer:	a packet replying to previous ask
//
// At any time there can be several Asks packets issued by both nodes.
// For an Ask packet a single Answer reply is expected		XXX vs protocol where there is one request and list of replies ?
//
// XXX -> multiple subconnection explicitly closed with ability to chat
// multiple packets without spawning goroutines? And only single answer
// expected implemented that after only ask-send / answer-receive the
// (sub-)connection is explicitly closed ?
//
// XXX it is maybe better to try to avoid multiplexing by hand and let the OS do it?
//
// A reply to particular Ask packet, once received, will be delivered to
// corresponding goroutine which originally issued Ask	XXX this can be put into interface
//
// TODO text
//
// TODO goroutine guarantees
//
//
//
// XXX naming (-> Conn ?, PeerLink ?)
type NodeLink struct {
	peerLink net.Conn		// raw conn to peer
	connTab  map[uint32]XXX		// msgid -> connection associated with msgid
}

// Conn is a sub-connection established over NodeLink
// TODO text
type Conn struct {
	rxq	chan Pkt	// XXX chan &Pkt ?
}

// Send notify packet to peer
func (c *NodeLink) Notify(pkt XXX) error {
	// TODO
}

// Send packet and wait for replied answer packet
func (c *NodeLink) Ask(pkt XXX) (answer Pkt, err error) {
	// TODO
}

// TODO how handle receive
// -> TODO Answer

// handle incoming packets routing them to either appropriate
// already-established connections or to new serving goroutine
func (c *NodeLink) serveReceive() error {
	for {
		// receive 1 packet
		pkt, err := c.recvPkt()
		if err != nil {
			panic(err)	// XXX err
		}

		// if we don't yet have connection established for pkt.Id spawn connection-serving goroutine
		// XXX connTab locking
		// TODO also check for conn ready to handle new packets, e.g. marked with msgid = 0
		//	(to avoid spawning goroutine each time for new packet)
		conn := c.connTab[pkt.Id]
		if conn != nil {
			go ...
		}

		// route packet to serving goroutine handler
		conn.rxq <- pkt
	}
}

// information about (received ?) packet
// XXX place?
type Pkt struct {
	PktHead
	Body	[]byte
}

// receive 1 packet from peer
func (c *NodeLink) recvPkt() (pkt Pkt, err error) {
	// TODO organize rx buffers management (freelist etc)

	// first read to read pkt header and hopefully up to page of data in 1 syscall
	rxbuf := make([]byte, 4096)
	n, err := io.ReadAtLeast(c.peerLink, rxbuf, PktHeadLen)
	if err != nil {
		panic(err)	// XXX err
	}

	pkt.Id     = binary.BigEndian.Uint32(rxbuf[0:])	// XXX -> PktHeader.Decode() ?
	pkt.Code   = binary.BigEndian.Uint16(rxbuf[4:])
	pkt.Length = binary.BigEndian.Uint32(rxbuf[6:])

	if pkt.Length < PktHeadLen {
		panic("TODO pkt.Length < PktHeadLen")	// XXX err	(length is a whole packet len with header)
	}
	if pkt.Length > MAX_PACKET_SIZE {
		panic("TODO message too big")	// XXX err
	}

	if pkt.Length > uint32(len(rxbuf)) {
		// grow rxbuf
		rxbuf2 := make([]byte, pkt.Length)
		copy(rxbuf2, rxbuf[:n])
		rxbuf = rxbuf2
	}

	// read rest of pkt data, if we need to
	_, err = io.ReadFull(c.peerLink, rxbuf[n:pkt.Length])
	if err != nil {
		panic(err)	// XXX err
	}

	return pkt, nil
}
