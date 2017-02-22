// XXX license/copyright

package fs1

// one entry inside transaction
type txnEntry struct {
	header	DataHeader
	data	[]byte
}

type dbEntry struct {
	header	TxnHeader
	// TODO user, desc, ext
	Recordv	[]txnEntry
}
