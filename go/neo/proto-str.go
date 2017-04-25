//go:generate stringer -output proto-str2.go -type ErrorCode proto.go

package neo

// XXX or better translate to some other errors ?
// XXX here - not in proto.go - because else stringer will be confused
func (e *Error) Error() string {
	s := e.Code.String()
	if e.Message != "" {
		s += ": " + e.Message
	}
	return s
}
