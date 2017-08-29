package client
// misc utilities

import (
	"bytes"
	"compress/zlib"
	"context"
	"io"

	"lab.nexedi.com/kirr/neo/go/xcommon/log"
)

// lclose closes c and logs closing error if there was any.
// the error is otherwise ignored
// XXX dup in neo,server
func lclose(ctx context.Context, c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Error(ctx, err)
	}
}

// decompress decompresses data according to zlib encoding.
//
// out buffer, if there is enough capacity, is used for decompression destination.
// if out has not not enough capacity a new buffer is allocated and used.
//
// return: destination buffer with full decompressed data or error.
func decompress(in []byte, out []byte) ([]byte, error) {
	bin := bytes.NewReader(in)
	zr, err := zlib.NewReader(bin)
	if err != nil {
		return nil, err
	}
	defer zr.Close()

	bout := bytes.NewBuffer(out)
	_, err = io.Copy(bout, bin)
	if err != nil {
		return nil, err
	}

	return bout.Bytes(), nil
}
