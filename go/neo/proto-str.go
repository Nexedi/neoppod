//go:generate stringer -output proto-str2.go -type ErrorCode,NodeType proto.go

package neo

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// XXX or better translate to some other errors ?
// XXX here - not in proto.go - because else stringer will be confused
func (e *Error) Error() string {
	s := e.Code.String()
	if e.Message != "" {
		s += ": " + e.Message
	}
	return s
}


const nodeTypeChar = "MSCA4567"	// keep in sync with NodeType constants

func (nodeUUID NodeUUID) String() string {
	// return ex 'S1', 'M2', ...
	if nodeUUID == 0 {
		return "?0"
	}

	typ := nodeUUID >> 24
	num := nodeUUID & (1<<24 - 1)

	temp := typ&(1 << 7) != 0
	typ &= 1<<7 - 1

	nodeType := NodeType(typ >> 4)
	s := fmt.Sprintf("%c%d", nodeTypeChar[nodeType], num)

	// 's1', 'm2', for temporary nids
	if temp {
		s = strings.ToLower(s)
	}

	return s
}

// ----------------------------------------

// Addr converts network address string into NEO Address
// TODO make neo.Address just string without host:port split
func AddrString(network, addr string) (Address, error) {
	// e.g. on unix, pipenet, etc networks there is no host/port split - the address there
	// is single string -> we put it into .Host and set .Port=0 to indicate such cases
	if strings.HasPrefix(network, "tcp") || strings.HasPrefix(network, "udp") {
		// networks that have host:port split
		host, portstr, err := net.SplitHostPort(addr)
		if err != nil {
			return Address{}, err
		}
		// XXX also lookup portstr in /etc/services (net.LookupPort) ?
		port, err := strconv.ParseUint(portstr, 10, 16)
		if err != nil {
			return Address{}, &net.AddrError{Err: "invalid port", Addr: addr}
		}

		return Address{Host: host, Port: uint16(port)}, nil

	}

	return Address{Host: addr, Port: 0}, nil
}

// Addr converts net.Addr into NEO Address
func Addr(addr net.Addr) (Address, error) {
	return AddrString(addr.Network(), addr.String())
}

// String formats Address to networked address string
func (addr Address) String() string {
	// XXX in py if .Host == "" -> whole Address is assumed to be empty

	// see Addr ^^^ about .Port=0 meaning no host:port split was applied
	switch addr.Port {
	case 0:
		return addr.Host

	default:
		return net.JoinHostPort(addr.Host, fmt.Sprintf("%d", addr.Port))
	}
}
