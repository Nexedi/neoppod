// TODO copyright / license

// neostorage - run a storage node of NEO

package main

import (
	_ "../../storage"	// XXX rel ok?
	neo "../.."
	"fmt"
)


// TODO options:
// cluster, masterv, bind ...

func main() {
	var t neo.Tid = neo.MAX_TID
	fmt.Printf("%T %x\n", t, t)
	println("TODO")
}
