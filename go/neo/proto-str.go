//go:generate stringer -output proto-str2.go -type ErrorCode proto.go

package neo

import (
	"fmt"
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


const nodeTypeChar = "MSCA4567"

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
