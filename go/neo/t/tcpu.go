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

// tcpu - cpu-related benchmarks
package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"hash"
	"hash/adler32"
	"hash/crc32"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

func dieusage() {
	fmt.Fprintf(os.Stderr, "Usage: tcpu <benchmark> <block-size>\n")
	os.Exit(1)
}

const unitv = "BKMGT" // (2^10)^i represents by corresponding char suffix

// fmtsize formats size in human readable form
func fmtsize(size int) string {
	const order = 1<<10
	norder := 0
	for size != 0 && (size % order) == 0 && (norder + 1 < len(unitv)) {
		size /= order
		norder += 1
	}

	return fmt.Sprintf("%d%c", size, unitv[norder])
}

func prettyarg(arg string) string {
	size, err := strconv.Atoi(arg)
	if err != nil {
		return arg
	}
	return fmtsize(size)
}

// benchit runs the benchmark for benchf
func benchit(benchname string, bencharg string, benchf func(*testing.B, string)) {
	// FIXME testing.Benchmark does not allow to detect whether benchmark failed.
	// (use log.Fatal, not {t,b}.Fatal as workaround)
	r := testing.Benchmark(func (b *testing.B) {
		benchf(b, bencharg)
	})

	fmt.Printf("Benchmark%s/go/%s %d\t%.3f µs/op\n", benchname, prettyarg(bencharg), r.N, float64(r.T) / float64(r.N) / float64(time.Microsecond))

}


func benchHash(b *testing.B, h hash.Hash, arg string) {
	blksize, err := strconv.Atoi(arg)
	if err != nil {
		log.Fatal(err)
	}

	data := make([]byte, blksize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h.Write(data)
	}
}

func BenchmarkAdler32(b *testing.B, arg string) { benchHash(b, adler32.New(), arg) }
func BenchmarkCrc32(b *testing.B, arg string)   { benchHash(b, crc32.NewIEEE(), arg) }
func BenchmarkSha1(b *testing.B, arg string)    { benchHash(b, sha1.New(), arg) }


var benchv = map[string]func(*testing.B, string) {
	"adler32":	BenchmarkAdler32,
	"crc32":	BenchmarkCrc32,
	"sha1":		BenchmarkSha1,
}


func main() {
	flag.Parse()	// so that test.* flags could be processed
	argv := flag.Args()
	if len(argv) != 2 {
		dieusage()
	}
	benchname := argv[0]
	bencharg  := argv[1]

	benchf, ok := benchv[benchname]
	if !ok {
		log.Fatalf("Unknown benchmark %q", benchname)
	}

	benchit(benchname, bencharg, benchf)
}
