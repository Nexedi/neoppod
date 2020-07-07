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
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	"lab.nexedi.com/kirr/neo/go/internal/xtesting"
	"lab.nexedi.com/kirr/neo/go/zodb"
	_ "lab.nexedi.com/kirr/neo/go/zodb/storage/fs1"

	"lab.nexedi.com/kirr/go123/exc"
	"lab.nexedi.com/kirr/go123/xerr"
	"lab.nexedi.com/kirr/go123/xnet"
)

// ZEOSrv represents running ZEO server.
type ZEOSrv interface {
	Addr() string // unix-socket address of the server
	Close() error
}

// ZEOPySrv represents running ZEO/py server.
//
// Create it with StartZEOPySrv(fs1path).
type ZEOPySrv struct {
	pysrv   *exec.Cmd	// spawned `runzeo -f fs1path`
	fs1path string		// filestorage location
	opt     ZEOPyOptions	// options for spawned server
	cancel  func()		// to stop pysrv
	done    chan struct{}	// ready after Wait completes
	errExit error		// error from Wait
}

type ZEOPyOptions struct {
}

// StartZEOPySrv starts ZEO/py server for FileStorage database located at fs1path.
func StartZEOPySrv(fs1path string, opt ZEOPyOptions) (_ *ZEOPySrv, err error) {
	defer xerr.Contextf(&err, "startzeo %s", fs1path)

	ctx, cancel := context.WithCancel(context.Background())

	z := &ZEOPySrv{fs1path: fs1path, cancel: cancel, done: make(chan struct{})}
	z.pysrv = exec.CommandContext(ctx, "python", "-m", "ZEO.runzeo", "-f", fs1path, "-a", z.Addr())
	z.opt = opt
	z.pysrv.Stdin = nil
	z.pysrv.Stdout = os.Stdout
	z.pysrv.Stderr = os.Stderr
	err = z.pysrv.Start()
	if err != nil {
		return nil, err
	}
	go func() {
		z.errExit = z.pysrv.Wait()
		close(z.done)
	}()
	defer func() {
		if err != nil {
			z.Close()
		}
	}()

	// wait till spawned ZEO is ready to serve clients
	for {
		select {
		default:
		case <-z.done:
			return nil, z.errExit
		}

		_, err := os.Stat(z.Addr())
		if err == nil {
			break // ZEO socket appeared
		}
		if os.IsNotExist(err) {
			err = nil // not yet
		}
		if err != nil {
			return nil, err
		}

		time.Sleep(100*time.Millisecond)
	}

	return z, nil
}

func (z *ZEOPySrv) Addr() string {
	return z.fs1path + ".zeosock"
}

func (z *ZEOPySrv) Close() (err error) {
	defer xerr.Contextf(&err, "stopzeo %s", z.fs1path)

	z.cancel()
	<-z.done
	err = z.errExit
	if _, ok := err.(*exec.ExitError); ok {
		err = nil // ignore exit statue - it is always !0 on kill
	}
	return err
}


// ----------------

// tOptions represents options for testing.
type tOptions struct {
	Preload string // preload database with data from this location
}

// withZEOSrv tests f with all kind of ZEO servers.
func withZEOSrv(t *testing.T, f func(t *testing.T, zsrv ZEOSrv), optv ...tOptions) {
	t.Helper()

	opt := tOptions{}
	if len(optv) > 1 {
		panic("multiple tOptions not allowed")
	}
	if len(optv) == 1 {
		opt = optv[0]
	}

	// withFS1 runs f under environment with new FileStorage database.
	withFS1 := func(t *testing.T, f func(fs1path string)) {
		t.Helper()
		X := xtesting.FatalIf(t)
		work := xtempdir(t)
		defer os.RemoveAll(work)
		fs1path := work + "/1.fs"

		if opt.Preload != "" {
			data, err := ioutil.ReadFile(opt.Preload); X(err)
			err = ioutil.WriteFile(fs1path, data, 0644); X(err)
		}

		f(fs1path)
	}

	// ZEO/py
	t.Run("py", func(t *testing.T) {
		t.Helper()
		xtesting.NeedPy(t, "ZEO")
		withFS1(t, func(fs1path string) {
			X := xtesting.FatalIf(t)

			zpy, err := StartZEOPySrv(fs1path, ZEOPyOptions{}); X(err)
			defer func() {
				err := zpy.Close(); X(err)
			}()

			f(t, zpy)
		})
	})
}

// withZEO tests f on all kinds of ZEO servers connected to by ZEO client.
func withZEO(t *testing.T, f func(t *testing.T, zdrv *zeo), optv ...tOptions) {
	t.Helper()
	withZEOSrv(t, func(t *testing.T, zsrv ZEOSrv) {
		t.Helper()
		X := xtesting.FatalIf(t)
		zdrv, _, err := zeoOpen(zsrv.Addr(), &zodb.DriverOptions{ReadOnly: true}); X(err)
		defer func() {
			err := zdrv.Close(); X(err)
		}()

		f(t, zdrv)
	}, optv...)
}

func TestHandshake(t *testing.T) {
	X := xtesting.FatalIf(t)
	withZEOSrv(t, func(t *testing.T, zsrv ZEOSrv) {
		ctx := context.Background()
		net := xnet.NetPlain("unix")
		zlink, err := dialZLink(ctx, net, zsrv.Addr()); X(err)
		defer func() {
			err := zlink.Close(); X(err)
		}()

		// conntected ok
	})
}

func TestLoad(t *testing.T) {
	X := exc.Raiseif

	data := "../fs1/testdata/1.fs"
	txnvOk, err := xtesting.LoadDBHistory(data); X(err)

	withZEO(t, func(t *testing.T, z *zeo) {
		xtesting.DrvTestLoad(t, z, txnvOk)
	}, tOptions{
		Preload: data,
	})
}


func zeoOpen(zurl string, opt *zodb.DriverOptions) (_ *zeo, at0 zodb.Tid, err error) {
	defer xerr.Contextf(&err, "openzeo %s", zurl)
	u, err := url.Parse(zurl)
	if err != nil {
		return nil, 0, err
	}

	z, at0, err := openByURL(context.Background(), u, opt)
	if err != nil {
		return nil, 0, err
	}

	return z.(*zeo), at0, nil
}

func xtempdir(t *testing.T) string {
	t.Helper()
	tmpd, err := ioutil.TempDir("", "zeo")
	if err != nil {
		t.Fatal(err)
	}
	return tmpd
}
