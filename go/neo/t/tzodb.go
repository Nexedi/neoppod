// Copyright (C) 2017-2018  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
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

// tzodb - ZODB-related benchmarks
package main

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"flag"
	"fmt"
	"io"
	"hash"
	"hash/crc32"
	"hash/adler32"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"lab.nexedi.com/kirr/go123/prog"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/neo/go/xcommon/log"
	"lab.nexedi.com/kirr/neo/go/xcommon/task"

	"lab.nexedi.com/kirr/neo/go/zodb"
	_ "lab.nexedi.com/kirr/neo/go/zodb/wks"

	"github.com/pkg/errors"
)

// hasher is hash.Hash which also knows its name
type hasher struct {
	hash.Hash
	name      string
}

// hasher that discards data
type nullHasher struct {}

func (h nullHasher) Write(b []byte) (int, error)	{ return len(b), nil }
func (h nullHasher) Sum(b []byte) []byte		{ return []byte{0} }
func (h nullHasher) Reset()				{}
func (h nullHasher) Size() int				{ return 1 }
func (h nullHasher) BlockSize() int			{ return 1 }


// hashFlags installs common hash flags and returns function to retrieve selected hasher.
func hashFlags(ctx context.Context, flags *flag.FlagSet) func() hasher {
	fnull    := flags.Bool("null",	false, "don't compute hash - just read data")
	fadler32 := flags.Bool("adler32",false, "compute Adler32 checksum")
	fcrc32   := flags.Bool("crc32",	false, "compute CRC32 checksum")
	fsha1    := flags.Bool("sha1",	false, "compute SHA1 cryptographic hash")
	fsha256  := flags.Bool("sha256", false, "compute SHA256 cryptographic hash")
	fsha512  := flags.Bool("sha512", false, "compute SHA512 cryptographic hash")

	return func() hasher {
		var h hasher
		inith := func(name string, ctor func() hash.Hash) {
			if h.name != "" {
				log.Fatalf(ctx, "duplicate hashes: %s and %s specified", h.name, name)
			}

			h.name = name
			h.Hash = ctor()
		}

		switch {
		case *fnull:	inith("null",	 func() hash.Hash { return nullHasher{} })
		case *fadler32:	inith("adler32", func() hash.Hash { return adler32.New() })
		case *fcrc32:	inith("crc32",   func() hash.Hash { return crc32.NewIEEE() })
		case *fsha1:	inith("sha1",    sha1.New)
		case *fsha256:	inith("sha256",  sha256.New)
		case *fsha512:	inith("sha512",  sha512.New)
		}

		return h
	}
}


// ----------------------------------------

const zhashSummary = "compute hash of whole latest objects stream in a ZODB database."

func zhashUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: tzodb zhash [options] <url>
`)
}

func zhashMain(argv []string) {
	ctx := context.Background()
	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.Usage = func() { zhashUsage(os.Stderr); flags.PrintDefaults() }

	fhash    := hashFlags(ctx, flags)
	fcheck   := flags.String("check", "",   "verify resulting hash to be = expected")
	fbench   := flags.String("bench", "",   "use benchmarking format for output")
	useprefetch := flags.Bool("useprefetch", false, "prefetch loaded objects")

	flags.Parse(argv[1:])

	if flags.NArg() != 1 {
		flags.Usage()
		os.Exit(1)
	}

	url := flags.Arg(0)

	h := fhash()
	if h.Hash == nil {
		log.Fatal(ctx, "no hash function specified")
	}

	err := zhash(ctx, url, h, *useprefetch, *fbench, *fcheck)
	if err != nil {
		log.Fatal(ctx, err)
	}
}

func zhash(ctx context.Context, url string, h hasher, useprefetch bool, bench, check string) (err error) {
	defer task.Running(&ctx, "zhash")(&err)

	stor, err := zodb.OpenStorage(ctx, url, &zodb.OpenOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() {
		err2 := stor.Close()
		err = xerr.First(err, err2)
	}()

	const nprefetch = 128	// XXX -> 512 ?

	// prefetchBlk prefetches block of nprefetch objects starting from xid
	//var tprevLoadBlkStart time.Time
	prefetchBlk := func(ctx context.Context, xid zodb.Xid) {
		if !useprefetch {
			return
		}

		//t1 := time.Now()
		for i := 0; i < nprefetch; i++ {
			//fmt.Printf("prefetch %v\n", xid)
			stor.Prefetch(ctx, xid)
			xid.Oid++
		}
		//t2 := time.Now()
		//δt := t2.Sub(t1)
		//fmt.Printf("tprefetch: %s", δt)
		//if !tprevLoadBlkStart.IsZero() {
		//	fmt.Printf("\ttprevload: %s", t1.Sub(tprevLoadBlkStart))
		//}
		//fmt.Printf("\n")
		//
		//tprevLoadBlkStart = t2
	}



	lastTid, err := stor.LastTid(ctx)
	if err != nil {
		return err
	}

	tstart := time.Now()

	oid := zodb.Oid(0)
	nread := 0
loop:
	for {
		xid := zodb.Xid{Oid: oid, At: lastTid}
		if xid.Oid % nprefetch == 0 {
			prefetchBlk(ctx, xid)
		}
		buf, _, err := stor.Load(ctx, xid)
		if err != nil {
			switch errors.Cause(err).(type) {
			case *zodb.NoObjectError:
				break loop
			default:
				return err
			}
		}

		h.Write(buf.Data)

		//fmt.Fprintf(os.Stderr, "%d @%s\t%s: %x\n", uint(oid), serial, h.name, h.Sum(nil))
		//fmt.Fprintf(os.Stderr, "\tdata: %x\n", buf.Data)

		nread += len(buf.Data)
		oid += 1

		buf.Release()
	}

	tend := time.Now()
	δt := tend.Sub(tstart)

	x := "zhash.go"
	if useprefetch {
		x += fmt.Sprintf("+prefetch%d", nprefetch)
	}
	hresult := fmt.Sprintf("%s:%x", h.name, h.Sum(nil))
	if bench == "" {
		fmt.Printf("%s   ; oid=0..%d  nread=%d  t=%s (%s / object)  x=%s\n",
			hresult, oid-1, nread, δt, δt / time.Duration(oid), x) // XXX /oid cast ?
	} else {
		topic := fmt.Sprintf(bench, x)	// XXX -> better text/template
		fmt.Printf("Benchmark%s %d %.1f µs/object\t# %s  nread=%d  t=%s\n",
			topic, oid-1, float64(δt) / float64(oid) / float64(time.Microsecond),
			hresult, nread, δt)
	}

	if check != "" && hresult != check {
		return fmt.Errorf("%s: hash mismatch: expected %s  ; got %s\t# x=%s", url, check, hresult, x)
	}

	return nil
}


// ----------------------------------------

const zwrkSummary = "benchmark database under parallel load from multiple clients."

func zwrkUsage(w io.Writer) {
	fmt.Fprintf(w,
`Usage: tzodb zwrk [options] <url>
`)
}

func zwrkMain(argv []string) {
	ctx := context.Background()
	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.Usage = func() { zwrkUsage(os.Stderr); flags.PrintDefaults() }

	fhash    := hashFlags(ctx, flags)
	fcheck   := flags.String("check", "",   "verify whole-database hash to be = expected")
	fbench   := flags.String("bench", "",   "benchmarking format for output")
	fnclient := flags.Int("nclient", 1,     "simulate so many clients")

	flags.Parse(argv[1:])

	if flags.NArg() != 1 {
		flags.Usage()
		os.Exit(1)
	}

	// XXX kill -bench and use labels in neotest
	if *fbench == "" {
		log.Fatal(ctx, "-bench must be provided")
	}

	url := flags.Arg(0)

	h := fhash()
	if (*fcheck != "") != (h.Hash != nil) {
		log.Fatal(ctx, "-check and -<hash> must be used together")
	}

	err := zwrk(ctx, url, *fnclient, h, *fbench, *fcheck)
	if err != nil {
		log.Fatal(ctx, err)
	}
}


// zwrk simulates database load from multiple clients.
//
// It first serially reads all objects and remember theirs per-object crc32
// checksum. If h/check were provided this phase, similarly to zhash, also
// checks that the whole database content is as expected.
//
// Then parallel phase starts where nwrk separate connections to database are
// opened and nwrk workers are run to perform database access over them in
// parallel to each other.
//
// At every time when an object is loaded its crc32 checksum is verified to be
// the same as was obtained during serial phase.
func zwrk(ctx context.Context, url string, nwrk int, h hasher, bench, check string) (err error) {
	at, objcheckv, err := zwrkPrepare(ctx, url, h, check)
	if err != nil {
		return err
	}

	// parallel phase
	defer task.Runningf(&ctx, "zwrk-%d", nwrk)(&err)
	ctx0 := ctx

	// establish nwrk connections and warm them up
	storv := make([]zodb.IStorage, nwrk)
	defer func() {
		for _, stor := range storv {
			if stor != nil {
				err2 := stor.Close()
				err = xerr.First(err, err2)
			}
		}
	}()

	wg, ctx := errgroup.WithContext(ctx0)
	for i := 0; i < nwrk; i++ {
		i := i
		wg.Go(func() error {
			// open storage without caching - we need to take
			// latency of every request into account, and a cache
			// could be inhibiting (e.g. making significantly
			// lower) it for some requests.
			var opts = zodb.OpenOptions{
				ReadOnly: true,
				NoCache:  true,
			}
			stor, err := zodb.OpenStorage(ctx, url, &opts)
			if err != nil {
				return err
			}
			storv[i] = stor

			// ping storage to warm-up the connection
			// (in case of NEO LastTid connects to master and Load - to storage)
			_, err = stor.LastTid(ctx)
			if err != nil {
				return err
			}

			buf, _, err := stor.Load(ctx, zodb.Xid{Oid: 0, At: at})
			buf.XRelease()
			if err != nil {
				return err
			}

			return nil
		})
	}

	err = wg.Wait()
	if err != nil {
		return err
	}

	// benchmark parallel loads
	r := testing.Benchmark(func (b *testing.B) {
		wg, ctx = errgroup.WithContext(ctx0)
		var n int64

		for i := 0; i < nwrk; i++ {
			stor := storv[i]
			oid  := zodb.Oid(0)

			wg.Go(func() error {
				for {
					n := atomic.AddInt64(&n, +1)
					if n >= int64(b.N) {
						return nil
					}

					xid := zodb.Xid{Oid: oid, At: at}
					buf, _, err := stor.Load(ctx, xid)
					if err != nil {
						return err
					}

					csum := crc32.ChecksumIEEE(buf.Data)
					if csum != objcheckv[oid] {
						return fmt.Errorf("%s: %s: crc32 mismatch: got %08x  ; expect %08x",
							url, oid, csum, objcheckv[oid])
					}

					buf.Release()

					// XXX various scenarios are possible to select next object to read
					oid = (oid + 1) % zodb.Oid(len(objcheckv))
				}

				return nil
			})
		}

		err = wg.Wait()
		if err != nil {
			// XXX log. (not b.) - workaround for testing.Benchmark
			// not allowing to detect failures.
			log.Fatal(ctx, err)
		}
	})

	// TODO latency distribution

	tavg   := float64(r.T) / float64(r.N) / float64(time.Microsecond)
	latavg := float64(nwrk) * tavg
	rps    := float64(r.N) / r.T.Seconds()
	topic := fmt.Sprintf(bench, "zwrk.go")
	fmt.Printf("Benchmark%s-%d %d\t%.1f req/s  %.3f latency-µs/object\n",
		topic, nwrk, r.N, rps, latavg)
	return nil
}


func zwrkPrepare(ctx context.Context, url string, h hasher, check string) (at zodb.Tid, objcheckv []uint32, err error) {
	defer task.Running(&ctx, "zwrk-prepare")(&err)

	stor, err := zodb.OpenStorage(ctx, url, &zodb.OpenOptions{ReadOnly: true})
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		err2 := stor.Close()
		err = xerr.First(err, err2)
	}()

	lastTid, err := stor.LastTid(ctx)
	if err != nil {
		return 0, nil, err
	}

	oid := zodb.Oid(0)
loop:
	for {
		xid := zodb.Xid{Oid: oid, At: lastTid}
		buf, _, err := stor.Load(ctx, xid)
		if err != nil {
			switch errors.Cause(err).(type) {
			case *zodb.NoObjectError:
				break loop
			default:
				return 0, nil, err
			}
		}

		// XXX Castagnoli is more strong and faster to compute
		objcheckv = append(objcheckv, crc32.ChecksumIEEE(buf.Data))

		if check != "" {
			h.Write(buf.Data)
		}
		oid += 1
		buf.Release()
	}


	// check the data read serially is indeed what was expected.
	if check != "" {
		hresult := fmt.Sprintf("%s:%x", h.name, h.Sum(nil))
		if hresult != check {
			return 0, nil, fmt.Errorf("%s: hash mismatch: expected %s  ; got %s", url, check, hresult)
		}
	}

	return lastTid, objcheckv, nil
}

// ----------------------------------------

var commands = prog.CommandRegistry{
	{"zhash", zhashSummary, zhashUsage, zhashMain},
	{"zwrk",  zwrkSummary,  zwrkUsage,  zwrkMain},
}

func main() {
	prog := prog.MainProg{
		Name:		"tzodb",
		Summary:	"tzodb is a tool to run ZODB-related benchmarks",
		Commands:	commands,
		HelpTopics:	nil,
	}

	defer log.Flush()
	prog.Main()
}
