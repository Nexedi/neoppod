package client

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

var ztestv = []struct{in, out string}{
	{
		in:  "x\x9c\xf3H\xcd\xc9\xc9W\x08\xcf/\xcaIQ\x04\x00\x1cI\x04>",
		out: "Hello World!",
	},
	{
		in:  "x\x9cK.H-*\xce,.I\xcd+\xd1\xcbM,(\xc8\xccK\xe7\n\x80\x0b\xf9BE\n\x19\xf5j\x0b\x99BYR\x12K\x12\x0b\x99k\x0bYB\xd9\x8b3\xd3\xf3\x12s\xca\nY5B9\x18 \x80\xb1\x90-\xb9<5/%5'3O/)3=\xb1\xa8(\xb1R\x0fL\xc6W\xe5\xa7$qE9e\xa6;\x82\xb8\\\x85\xec%\x81\xc5\xc5z\x00\xb0d)\xef",
		out: "cpersistent.mapping\nPersistentMapping\nq\x01.}q\x02U\x04dataq\x03}q\x04U\x07signalvq\x05(U\x08\x00\x00\x00\x00\x00\x00\x00\x01q\x06cwendelin.bigarray.array_zodb\nZBigArray\nq\x07tQss.",
	},
}

func TestDecompress(t *testing.T) {
	for _, tt := range ztestv {
		got, err := decompress([]byte(tt.in), nil)
		if err != nil {
			t.Errorf("decompress err: %q", tt.in)
			continue
		}
		gots := string(got)
		if gots != tt.out {
			t.Errorf("decompress output mismatch:\n%s\n",
				pretty.Compare(tt.out, gots))
		}
	}
}