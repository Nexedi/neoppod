// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

// +build ignore

// tsha1 - benchmark sha1
package main

import (
	"crypto/sha1"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func dieusage() {
	fmt.Fprintf(os.Stderr, "Usage: tsha1 <block-size>\n")
	os.Exit(1)
}

func main() {
	if len(os.Args) != 2 {
		dieusage()
	}
	blksize, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	data := make([]byte, blksize)
	h := sha1.New()

	n := int(1E6)
	if blksize > 1024 {
		n = n * 1024 / blksize	// assumes 1K ~= 1μs
	}

	tstart := time.Now()

	for i := 0; i < n; i++ {
		h.Write(data)
	}

	tend := time.Now()
	δt := tend.Sub(tstart)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "?"
	}

	fmt.Printf("Benchmarksha1/%s/go/%dB %d\t%.3f µs/op\n", hostname, blksize, n, float64(δt) / float64(n) / float64(time.Microsecond))
}
