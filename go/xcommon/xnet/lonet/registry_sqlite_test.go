// Copyright (C) 2018  Nexedi SA and Contributors.
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

package lonet

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"lab.nexedi.com/kirr/go123/exc"
)

func TestRegistrySQLite(t *testing.T) {
	X := exc.Raiseif

	work, err := ioutil.TempDir("", "t-registry-sqlite")
	X(err)
	defer os.RemoveAll(work)

	dbpath := work + "/1.db"

	ctx := context.Background()

	r, err := openRegistrySQLite(ctx, dbpath)
	X(err)

	// quert checks that result of Query(hostname) is as expect
	//
	// if expect is error - it checks that Query returns error with cause == expect.
	// otherwise expect must be string and it will check that Query
	// succeeds and returns osladdr == expect.
	query := func(r *sqliteRegistry, hostname string, expect interface{}) {
		// XXX ^^^ -> `r registry` (needs .Network() to get network name) ?
		t.Helper()

		osladdr, err := r.Query(ctx, hostname)
		if ewant, iserr := expect.(error); iserr {
			// error expected
			// XXX construct full registry error around ewant + reflect.DeepCompare
			e, ok := err.(*registryError)
			if !(ok && e.Err == ewant && osladdr == "") {
				t.Fatalf("%s: query %q:\nwant: \"\", %v\nhave: %q, %v",
					r.uri, hostname, ewant, osladdr, err)
			}
		} else {
			// !error expected
			laddr := expect.(string)
			if !(osladdr == laddr && err == nil) {
				t.Fatalf("%s: query %q:\nwant: %q, nil\nhave: %q, %v",
					r.uri, hostname, laddr, osladdr, err)
			}
		}
	}

	// announce checks that result of Announce(hostname, osladdr) is as expected
	//
	// if len(errv) == 1 - it checks that Announce returns error with cause == errv[0].
	// otherwise it will check that Announce succeeds and returns nil error.
	announce := func(r *sqliteRegistry, hostname, osladdr string, errv ...error) {
		t.Helper()
		err := r.Announce(ctx, hostname, osladdr)
		var ewant error
		if len(errv) > 0 {
			ewant = errv[0]
			if len(errv) > 1 {
				panic("only 1 error allowed in announce check")
			}
		}
		if ewant != nil {
			// error expected
			// XXX construct full registry error around ewant + reflect.DeepCompare
			e, ok := err.(*registryError)
			if (!ok && e.Err == ewant) {
				t.Fatalf("%s: announce %q %q:\nwant %v\nhave: %v",
					r.uri, hostname, osladdr, ewant, err)
			}
		} else {
			// !error expected
			if err != nil {
				t.Fatalf("%s: announce %q %q: %s", r.uri, hostname, osladdr, err)
			}
		}
	}



	ø := errNoHost

	// r.Network() == ...
	query(r, "α", ø)
	announce(r, "α", "alpha:1234")
	announce(r, "α", "alpha:1234", errHostDup)
	announce(r, "α", "alpha:1235", errHostDup)
	query(r, "α", "alpha:1234")
	// r.Query("β") == ø

	r2, err := openRegistrySQLite(ctx, dbpath)
	// r2.Network() == ...
	// r2.Query("α") == "alpha:1234"
	// r2.Query("β") == ø
	// r2.Announce("β", "beta:zzz")
	// r2.Query("β") == "beta:zzz")

	// r.Query("β") == "beta:zzz")

	X(r.Close())

	// r.Query("α") == errRegistryDown
	// r.Query("β") == errRegistryDown
	// r.Announce("γ", "gamma:qqq") == errRegistryDown
	// r.Query("γ") == errRegistryDown

	X(r2.Close())
}
