// Copyright (C) 2020  Nexedi SA and Contributors.
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

package zeo

import (
	"context"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"

	"lab.nexedi.com/kirr/neo/go/internal/xtesting"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/xerr"
)

// ZEOPySrv represents running ZEO/py server.
//
// Create it with StartZEOPySrv(fs1path).
type ZEOPySrv struct {
	pysrv   *exec.Cmd	// spawned `runzeo -f fs1path`
	fs1path string
	cancel  func()		// to stop pysrv
}


func StartZEOPySrv(fs1path string) (_ *ZEOPySrv, err error) {
	defer xerr.Contextf(&err, "startzeo %s", fs1path)

	ctx, cancel := context.WithCancel(context.Background())

	z := &ZEOPySrv{fs1path: fs1path, cancel: cancel}
	z.pysrv = exec.CommandContext(ctx, "python", "-m", "ZEO.runzeo", "-f", fs1path, "-a", z.zaddr())
	z.pysrv.Stdin = nil
	z.pysrv.Stdout = os.Stdout
	z.pysrv.Stderr = os.Stderr
	// XXX cwd?
	err = z.pysrv.Start()
	if err != nil {
		return nil, err
	}

	return z, nil
}

// zaddr returns address of unix socket to access spawned ZEO server.
func (z *ZEOPySrv) zaddr() string {
	return z.fs1path + ".zeosock"
}

func (z *ZEOPySrv) Close() (err error) {
	defer xerr.Contextf(&err, "stopzeo %s", z.fs1path)

	z.cancel()
	return z.pysrv.Wait()
}


func TestWatch(t *testing.T) {
	X := exc.Raiseif
	xtesting.NeedPy(t, "ZEO") // XXX +msgpack?

	work := xtempdir(t)
	defer os.RemoveAll(work)

	fs1path := work + "/1.fs"
	zpy, err := StartZEOPySrv(fs1path); X(err)
	defer func() {
		err := zpy.Close(); X(err)
	}()

	xtesting.DrvTestWatch(t, zpy.zaddr(), openByURL)
}


func xtempdir(t *testing.T) string {
	t.Helper()
	tmpd, err := ioutil.TempDir("", "zeo")
	if err != nil {
		t.Fatal(err)
	}
	return tmpd
}
