// Copyright (C) 2017-2018  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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

//go:generate stringer -output zproto-str.go -type ErrorCode,ClusterState,NodeType,NodeState,CellState proto.go

package proto
// supporting code for types defined in proto.go

import (
	"fmt"
	"math"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// MsgType looks up message type by message code.
//
// Nil is returned if message code is not valid.
func MsgType(msgCode uint16) reflect.Type {
	return msgTypeRegistry[msgCode]
}


func (e *Error) Error() string {
	// NOTE here, not in proto.go - because else stringer will be confused.
	// XXX better translate to some other errors?
	s := e.Code.String()
	if e.Message != "" {
		s += ": " + e.Message
	}
	return s
}


// node type -> character representing it.
const nodeTypeChar = "SMCA" // NOTE neo/py does this out of sync with NodeType constants.

// String returns string representation of a node uuid.
//
// It returns ex 'S1', 'M2', ...
func (nodeUUID NodeUUID) String() string {
	if nodeUUID == 0 {
		return "?(0)0"
	}

	num := nodeUUID & (1<<24 - 1)

	// XXX UUID_NAMESPACES description does not match neo/py code
	//typ := nodeUUID >> 24
	//temp := typ&(1 << 7) != 0
	//typ &= 1<<7 - 1
	//nodeType := typ >> 4
	typ := uint8(-int8(nodeUUID>>24)) >> 4

	if typ < 4 {
		return fmt.Sprintf("%c%d", nodeTypeChar[typ], num)
	}

	return fmt.Sprintf("?(%d)%d", typ, num)

	/*
	// 's1', 'm2', for temporary nids
	if temp {
		s = strings.ToLower(s)
	}

	return s
	*/
}


// XXX goes out of sync wrt NodeType constants
var nodeTypeNum = [...]int8{
	STORAGE:  0x00,
	MASTER:  -0x10,
	CLIENT:  -0x20,
	ADMIN:   -0x30,
}
// UUID creates node uuid from node type and number.
func UUID(typ NodeType, num int32) NodeUUID {
	// XXX neo/py does not what UUID_NAMESPACES describes
	/*
	temp := uint32(0)
	if num < 0 {
		temp = 1
		num = -num
	}
	*/

	if int(typ) >= len(nodeTypeNum) {
		panic("typ invalid")
	}

	typn := nodeTypeNum[typ]

	if (num < 0) || num>>24 != 0 {
		panic("node number out of range")
	}

	//uuid := temp << (7 + 3*8) | uint32(typ) << (4 + 3*8) | uint32(num)
	uuid := uint32(uint8(typn))<<(3*8) | uint32(num)
	return NodeUUID(uuid)
}

// ----------------------------------------

// IdTimeNone represents None passed as identification time.
var IdTimeNone = IdTime(math.Inf(-1))

func (t IdTime) String() string {
	if float64(t) == math.Inf(-1) {
		return "ø"
	}

	sec := int64(t)
	nsec := int64((float64(t) - float64(sec)) * 1E9)
	return time.Unix(sec, nsec).String()
}

// ----------------------------------------

// AddrString converts network address string into NEO Address.
//
// TODO make neo.Address just string without host:port split
func AddrString(network, addr string) (Address, error) {
	// empty is always empty
	if addr == "" {
		return Address{}, nil
	}

	// e.g. on unix, networks there is no host/port split - the address there
	// is single string -> we put it into .Host and set .Port=0 to indicate such cases
	switch {
	default:
		return Address{Host: addr, Port: 0}, nil

	// networks that have host:port split
	case strings.HasPrefix(network, "tcp"):
	case strings.HasPrefix(network, "udp"):
	case strings.HasPrefix(network, "pipe"):
	}

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

// Addr converts net.Addr into NEO Address.
func Addr(addr net.Addr) (Address, error) {
	return AddrString(addr.Network(), addr.String())
}

// String formats Address to networked address string.
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
