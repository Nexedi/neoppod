// Code generated by "stringer -output proto-str2.go -type ErrorCode,NodeType proto.go"; DO NOT EDIT.

package neo

import "fmt"

const _ErrorCode_name = "ACKNOT_READYOID_NOT_FOUNDTID_NOT_FOUNDOID_DOES_NOT_EXISTPROTOCOL_ERRORREPLICATION_ERRORCHECKING_ERRORBACKEND_NOT_IMPLEMENTEDNON_READABLE_CELLREAD_ONLY_ACCESSINCOMPLETE_TRANSACTION"

var _ErrorCode_index = [...]uint8{0, 3, 12, 25, 38, 56, 70, 87, 101, 124, 141, 157, 179}

func (i ErrorCode) String() string {
	if i >= ErrorCode(len(_ErrorCode_index)-1) {
		return fmt.Sprintf("ErrorCode(%d)", i)
	}
	return _ErrorCode_name[_ErrorCode_index[i]:_ErrorCode_index[i+1]]
}

const _NodeType_name = "MASTERSTORAGECLIENTADMIN"

var _NodeType_index = [...]uint8{0, 6, 13, 19, 24}

func (i NodeType) String() string {
	if i < 0 || i >= NodeType(len(_NodeType_index)-1) {
		return fmt.Sprintf("NodeType(%d)", i)
	}
	return _NodeType_name[_NodeType_index[i]:_NodeType_index[i+1]]
}