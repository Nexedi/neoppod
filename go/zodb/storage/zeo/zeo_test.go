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
	"fmt"
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

	Encoding() byte // encoding used on the wire - 'M' or 'Z'
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
	msgpack bool // whether to advertise msgpack
}

// StartZEOPySrv starts ZEO/py server for FileStorage database located at fs1path.
func StartZEOPySrv(fs1path string, opt ZEOPyOptions) (_ *ZEOPySrv, err error) {
	defer xerr.Contextf(&err, "startzeo %s", fs1path)

	ctx, cancel := context.WithCancel(context.Background())

	z := &ZEOPySrv{fs1path: fs1path, cancel: cancel, done: make(chan struct{})}
	z.pysrv = exec.CommandContext(ctx, "python", "-m", "ZEO.runzeo", "-f", fs1path, "-a", z.Addr())
	z.opt = opt
	msgpack := ""
	if opt.msgpack {
		msgpack = "y"
	}
	z.pysrv.Env = append(os.Environ(), "ZEO_MSGPACK="+msgpack)
	z.pysrv.Stdin = nil
	z.pysrv.Stdout = os.Stdout
	z.pysrv.Stderr = os.Stderr
	// XXX cwd?
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

func (z *ZEOPySrv) Encoding() byte {
	encoding := byte('Z')
	if z.opt.msgpack { encoding = byte('M') }
	return encoding
}


// ----------------

// withZEOPySrv spawns new ZEO/py server and runs f in that environment.
func withZEOPySrv(t *testing.T, opt ZEOPyOptions, f func(zpy *ZEOPySrv)) {
	X := mkFatalIf(t)
	t.Helper()
	needZEOpy(t)

	work := xtempdir(t)
	defer os.RemoveAll(work)
	fs1path := work + "/1.fs"

	zpy, err := StartZEOPySrv(fs1path, opt); X(err)
	defer func() {
		err := zpy.Close(); X(err)
	}()

	f(zpy)
}

// withZEOSrv runs f under all kind of ZEO servers.
func withZEOSrv(t *testing.T, f func(t *testing.T, zsrv ZEOSrv)) {
	for _, msgpack := range []bool{false, true} {
		// ZEO/py
		t.Run(fmt.Sprintf("py/msgpack=%v", msgpack), func(t *testing.T) {
			withZEOPySrv(t, ZEOPyOptions{msgpack: msgpack}, func(zpy *ZEOPySrv) {
				f(t, zpy)
			})
		})

		// TODO ZEO/go
	}
}

func TestHandshake(t *testing.T) {
	X := mkFatalIf(t)
	withZEOSrv(t, func(t *testing.T, zsrv ZEOSrv) {
		ctx := context.Background()
		net := xnet.NetPlain("unix")
		zlink, err := dialZLink(ctx, net, zsrv.Addr()); X(err)
		defer func() {
			err := zlink.Close(); X(err)
		}()

		ewant := zsrv.Encoding()
		if zlink.encoding != ewant {
			t.Fatalf("handshake: encoding=%c  ; want %c", zlink.encoding, ewant)
		}
	})
}

func TestLoad(t *testing.T) {
	X := exc.Raiseif
	needZEOpy(t)

	work := xtempdir(t)
	defer os.RemoveAll(work)
	fs1path := work + "/1.fs"

	// copy ../fs1/testdata/1.fs -> fs1path
	data, err := ioutil.ReadFile("../fs1/testdata/1.fs");	X(err)
	err = ioutil.WriteFile(fs1path, data, 0644);		X(err)

	txnvOk, err := xtesting.LoadDB(fs1path); X(err)

	zpy, err := StartZEOPySrv(fs1path, ZEOPyOptions{}); X(err)
	defer func() {
		err := zpy.Close(); X(err)
	}()

	z, _, err := zeoOpen(zpy.Addr(), &zodb.DriverOptions{ReadOnly: true}); X(err)
	defer func() {
		err := z.Close(); X(err)
	}()

	xtesting.DrvTestLoad(t, z, txnvOk)
}

func TestWatch(t *testing.T) {
	withZEOSrv(t, func(t *testing.T, zsrv ZEOSrv) {
		xtesting.DrvTestWatch(t, "zeo://" + zsrv.Addr(), openByURL)
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

func mkFatalIf(t *testing.T) func(error) {
	return func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func needZEOpy(t *testing.T) {
	xtesting.NeedPy(t, "ZEO") // XXX +msgpack?
}
