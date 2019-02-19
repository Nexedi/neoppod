// Copyright (C) 2019  Nexedi SA and Contributors.
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

package zodbtools

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/neo/go/internal/xtesting"
	"lab.nexedi.com/kirr/neo/go/zodb"
)

func TestWatch(t *testing.T) {
	X := exc.Raiseif

	xtesting.NeedPy(t, "zodbtools")
	work, err := ioutil.TempDir("", "t-zodbwatch"); X(err)
	defer os.RemoveAll(work)
	tfs := work + "/t.fs"

	// force tfs creation
	at := zodb.Tid(0)
	xcommit := func(objv ...xtesting.ZRawObject) {
		t.Helper()
		var err error
		at, err = xtesting.ZPyCommitRaw(tfs, at, objv...)
		if err != nil {
			t.Fatal(err)
		}
	}

	xcommit(xtesting.ZRawObject{0, []byte("data0")})

	// spawn plain and verbose watchers
	ctx0, cancel := context.WithCancel(context.Background())
	wg, ctx := errgroup.WithContext(ctx0)

	// gowatch spawns Watch(verbose) and returns expect(line) func that is
	// connected to Watch output.
	gowatch := func(verbose bool) /*expectf*/func(format string, argv ...interface{}) {
		pr, pw := io.Pipe()
		wg.Go(func() error {
			stor, err := zodb.OpenStorage(ctx, tfs, &zodb.OpenOptions{ReadOnly: true})
			if err != nil {
				return err
			}
			return Watch(ctx, stor, pw, verbose)
		})

		r := bufio.NewReader(pr)
		expectf := func(format string, argv ...interface{}) {
			t.Helper()
			l, err := r.ReadString('\n')
			if err != nil {
				t.Fatalf("expect: %s", err)
			}
			l = l[:len(l)-1] // trim trailing \n
			line := fmt.Sprintf(format, argv...)
			if l != line {
				t.Fatalf("expect\nhave: %q\nwant: %q", l, line)
			}
		}
		return expectf
	}

	pexpect := gowatch(false)
	vexpect := gowatch(true)

	pexpect("# at %s", at)
	vexpect("# at %s", at)

	xcommit(xtesting.ZRawObject{0, []byte("data01")})

	pexpect("txn %s", at)
	vexpect("txn %s", at)
	vexpect("obj 0000000000000000")
	vexpect("")

	xcommit(xtesting.ZRawObject{1, []byte("data1")}, xtesting.ZRawObject{2, []byte("data2")})

	pexpect("txn %s", at)
	vexpect("txn %s", at)
	vexpect("obj 0000000000000001")
	vexpect("obj 0000000000000002")
	vexpect("")

	cancel()

	err = wg.Wait()
	ecause := errors.Cause(err)
	if ecause != context.Canceled {
		t.Fatalf("finished: err: expected 'canceled' cause; got %q", err)
	}
}
