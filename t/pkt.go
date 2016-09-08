package pkt

type Header struct {
	Id	uint32
	Code	uint16
	Len	uint32
}

type Notify struct {
	// Header
	Message	string
}
