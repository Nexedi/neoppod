// TODO copyright / license

package neo

import (
	"context"
)

// NEO Storage application

type StorageApplication struct {
}



// ----------------------------------------

// Server is an interface that represents networked server	XXX text
type Server interface {
	// ServeConn serves already established connection in a blocking way.
	// ServeConn is usually run in separate goroutine	XXX text
	ServeConn(ctx context.Context, conn net.Conn)	// XXX error ?
}

// srv.ServeConn(ctx context.Context, conn net.Conn)

// Run service on a listener
// - accept incoming connection on the listener
// - for every accepted connection spawn srv.ServeConn() in separate goroutine.
//
// the listener is closed when Serve returns.
// XXX text
// XXX move -> generic place ?
func Serve(ctx context.Context, l net.Listener, srv Server) error {
	// close listener when either cancelling or returning (e.g. due to an error)
	// ( when cancelling - listener close will signal to all accepts to
	//   terminate with an error )
	retch := make(chan struct{})
	defer func() { close(retch) }()
	go func(
		select {
		case <-ctx.Done():
		case <-retch:
		}
		l.Close() // XXX err
	)()

	// main Accept -> ServeConn loop
	for {
		conn, err := l.Accept()
		if err != nil {
			// TODO check done channel of l/srv somehow
			// TODO err -> net.Error && .Temporary() -> some throttling
			return err
		}

		go srv.ServeConn(ctx, conn)
	}
}

// TODO text
// XXX move -> generic place ?
// XXX get (net, laddr) from srv ?
// XXX split -> separate Listen()
func ListenAndServe(ctx context.Context, net, laddr string, srv Server) error {
	l, err := net.Listen(net, laddr)
	if err != nil {
		return err
	}
	// TODO set keepalive on l
	// TODO if TLS config -> tls.NewListener()
	return Serve(ctx, l, srv)
}
