// Copyright (C) 2017  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 2, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.

/*
fstail - Tool to dump the last few transactions from a FileStorage.

Format is the same as in fstail/py originally written by Jeremy Hylton:

	https://github.com/zopefoundation/ZODB/blob/master/src/ZODB/scripts/fstail.py
	https://github.com/zopefoundation/ZODB/commit/551122cc
*/

package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"../../../../storage/fs1"

	"lab.nexedi.com/kirr/go123/mem"
)

// pyQuote quotes string the way python repr(str) would do
func pyQuote(s string) string {
	out := pyQuoteBytes(mem.Bytes(s))
	return mem.String(out)
}

func pyQuoteBytes(b []byte) []byte {
	s := mem.String(b)
	buf := make([]byte, 0, len(s))

	// smartquotes: choose ' or " as quoting character
	// https://github.com/python/cpython/blob/v2.7.13-116-g1aa1803b3d/Objects/stringobject.c#L947
	quote := byte('\'')
	noquote := byte('"')
	if strings.ContainsRune(s, '\'') && !strings.ContainsRune(s, '"') {
		quote, noquote = noquote, quote
	}

	buf = append(buf, quote)

	for i, r := range s {
		switch r {
		case utf8.RuneError:
			buf = append(buf, []byte(fmt.Sprintf("\\x%02x", s[i]))...)
		case '\\', rune(quote):
			buf = append(buf, '\\', byte(r))
		case rune(noquote):
			buf = append(buf, noquote)

		// NOTE python converts to \<letter> only \t \n \r  (not e.g. \v)
		// https://github.com/python/cpython/blob/v2.7.13-116-g1aa1803b3d/Objects/stringobject.c#L963
		case '\t':
			buf = append(buf, `\t`...)
		case '\n':
			buf = append(buf, `\n`...)
		case '\r':
			buf = append(buf, `\r`...)

		default:
			switch {
			case r < ' ':
				// we already converted to \<letter> what python represents as such above
				buf = append(buf, []byte(fmt.Sprintf("\\x%02x", s[i]))...)

			default:
				// we already handled ', " and (< ' ') above, so now it
				// should be safe to reuse strconv.QuoteRune
				rq := strconv.QuoteRune(r)	// "'\x01'"
				rq = rq[1:len(rq)-1]		//  "\x01"
				buf = append(buf, rq...)
			}
		}
	}

	buf = append(buf, quote)
	return buf
}

func fsTail(w io.Writer, path string, ntxn int) (err error) {
	// path & fstail on error context
	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: fstail: %v", path, err)
		}
	}()

	// we are not using fs1.Open here, since the file or index could be
	// e.g. corrupt - we want to iterate directly by FileStorage records
	// (same logic as in fstail/py)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		err2 := f.Close()
		if err == nil {
			err = err2
		}
	}()

	// get file size as topPos
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	topPos := fi.Size()

	// txn header & data buffer for iterating
	txnh := fs1.TxnHeader{}
	data := []byte{}


	// TODO use SeqBufReader instead of f and check speedup

	// start iterating at tail.
	// this should get EOF but read txnh.LenPrev ok.
	err = txnh.Load(f, topPos, fs1.LoadAll)
	if err != io.EOF {
		if err == nil {
			// XXX or allow this?
			// though then, if we start not from a txn boundary - there are
			// high chances further iteration will go wrong.
			err = fmt.Errorf("@%v: reading past file end was unexpectedly successful: probably the file is being modified simultaneously", topPos)
		}
		// otherwise err already has the context
		return err
	}
	if txnh.LenPrev <= 0 {
		return fmt.Errorf("@%v: previous record could not be read", topPos)
	}

	// now loop loading transactions backwards until EOF / ntxn limit
	for i := ntxn; i > 0; i-- {
		err = txnh.LoadPrev(f, fs1.LoadAll)
		if err != nil {
			if err == io.EOF {
				err = nil	// XXX -> okEOF(err)
			}
			break
		}

		// read raw data inside transaction record
		dataLen := txnh.DataLen()
		if int64(cap(data)) < dataLen {
			data = make([]byte, dataLen)
		} else {
			data = data[:dataLen]
		}

		_, err = f.ReadAt(data, txnh.DataPos())
		if err != nil {
			// XXX -> txnh.Err(...) ?
			// XXX err = noEOF(err)
			err = &fs1.ErrTxnRecord{txnh.Pos, "read data payload", err}
			break
		}

		// print information about read txn record
		_, err = fmt.Fprintf(w, "%s: hash=%x\nuser=%s description=%s length=%d offset=%d (+%d)\n\n",
				txnh.Tid.Time(), sha1.Sum(data),

				// fstail.py uses repr to print user/description:
				// https://github.com/zopefoundation/ZODB/blob/5.2.0-4-g359f40ec7/src/ZODB/scripts/fstail.py#L39
				pyQuoteBytes(txnh.User), pyQuoteBytes(txnh.Description),

				// NOTE in zodb/py .length is len - 8, in zodb/go - whole txn record length
				txnh.Len - 8,

				txnh.Pos, txnh.HeaderLen())
		if err != nil {
			break
		}
	}

	return err
}


func main() {
	ntxn := 10

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
`fstail [options] <storage>
Dump transactions from a FileStorage in reverse order

<storage> is a path to FileStorage

  options:

	-h --help       this help text.
	-n <N>	        output the last <N> transactions (default %d).
`, ntxn)
	}

	flag.IntVar(&ntxn, "n", ntxn, "output the last <N> transactions")
	flag.Parse()

	argv := flag.Args()
	if len(argv) < 1 {
		flag.Usage()
		os.Exit(2)
	}
	storPath := argv[0]

	err := fsTail(os.Stdout, storPath, ntxn)
	if err != nil {
		log.Fatal(err)
	}
}
