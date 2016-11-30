// TODO copyright / license

package neo

import (
	"context"
	"net"
	"fmt"

	//"../neo/proto"
)

// NEO Storage application

// XXX naming
type StorageApplication struct {
}


/*
// XXX change to bytes.Buffer if we need to access it as I/O
type Buffer struct {
	buf	[]byte
}
*/

func (stor *StorageApplication) ServeConn(ctx context.Context, conn net.Conn) {
	fmt.Printf("stor: serving new client %s <-> %s\n", conn.LocalAddr(), conn.RemoteAddr())
	//fmt.Fprintf(conn, "Hello up there, you address is %s\n", conn.RemoteAddr())	// XXX err
	//conn.Close()	// XXX err

	/*
	// TODO: use bytes.Buffer{}
	//	 .Bytes() -> buf -> can grow up again up to buf[:cap(buf)]
	//	 NewBuffer(buf) -> can use same buffer for later reading via bytes.Buffer
	// TODO read PktHeader (fixed length)  (-> length, PktType (by .code))
	//rxbuf := bytes.Buffer{}
	rxbuf := bytes.NewBuffer(make([]byte, 4096))
	n, err := conn.Read(rxbuf.Bytes())
	*/

	recvPkt()
}



// ----------------------------------------

// Server is an interface that represents networked server	XXX text
type Server interface {
	// ServeConn serves already established connection in a blocking way.
	// ServeConn is usually run in separate goroutine	XXX text
	ServeConn(ctx context.Context, conn net.Conn)	// XXX error ?
}

// Run service on a listener
// - accept incoming connection on the listener
// - for every accepted connection spawn srv.ServeConn() in separate goroutine.
//
// the listener is closed when Serve returns.
// XXX move -> generic place ?
func Serve(ctx context.Context, l net.Listener, srv Server) error {
	fmt.Printf("stor: serving on %s ...\n", l.Addr())

	// close listener when either cancelling or returning (e.g. due to an error)
	// ( when cancelling - listener close will signal to all accepts to
	//   terminate with an error )
	retch := make(chan struct{})
	defer func() { close(retch) }()
	go func() {
		select {
		case <-ctx.Done():
		case <-retch:
		}
		l.Close() // XXX err
	}()

	// main Accept -> ServeConn loop
	for {
		conn, err := l.Accept()
		if err != nil {
			// TODO err == closed <-> ctx was cancelled
			// TODO err -> net.Error && .Temporary() -> some throttling
			return err
		}

		go srv.ServeConn(ctx, conn)
	}
}

// TODO text
// XXX move -> generic place ?
// XXX split -> separate Listen() & Serve()
func ListenAndServe(ctx context.Context, net_, laddr string, srv Server) error {
	l, err := net.Listen(net_, laddr)
	if err != nil {
		return err
	}
	// TODO set keepalive on l
	// TODO if TLS config -> tls.NewListener()
	return Serve(ctx, l, srv)
}
