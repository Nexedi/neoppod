// TODO copyright / license

// neostorage - run a storage node of NEO

package main

import (
	_ "../../storage"	// XXX rel ok?
	neo "../.."
	"fmt"
	"context"
	"time"
)


// TODO options:
// cluster, masterv, bind ...

func main() {
	var t neo.Tid = neo.MAX_TID
	fmt.Printf("%T %x\n", t, t)
	println("TODO")

	storsrv := &neo.StorageApplication{}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Second)
		cancel()
	}()
	//ctx := context.Background()

	err := neo.ListenAndServe(ctx, "tcp", "localhost:1234", storsrv)
	fmt.Println(err)
}
