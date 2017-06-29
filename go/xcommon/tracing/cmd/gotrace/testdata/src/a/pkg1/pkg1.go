package pkg1

import (
	"net/url"

	// extra import which is used in package but should not be used in tracing
	"fmt"
)

// probe receives no args
//trace:event traceNewTPre()

// probe receives type this package defines
//trace:event traceNewT(t *T)

type T struct {}

func NewT() *T {
	traceNewTPre()
	t := &T{}
	traceNewT(t)
	return t
}

// probe receives type from another package
//trace:event traceURLParsed(u *url.URL)

func ParseURL(ustr string) (*url.URL, error) {
	u, err := url.Parse(ustr)
	if err != nil {
		return nil, fmt.Errorf("oh my bad: %v", err)
	}

	traceURLParsed(u)
	return u, nil
}

// probe receives builtin type
//trace:event traceDoSomething(topic string)

func DoSomething(topic string) {
	traceDoSomething(topic)
}
