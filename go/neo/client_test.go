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

package neo

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	"lab.nexedi.com/kirr/neo/go/internal/xexec"
	"lab.nexedi.com/kirr/neo/go/internal/xtesting"
	"lab.nexedi.com/kirr/neo/go/zodb"

	"lab.nexedi.com/kirr/go123/xerr"
)

// NEOSrv represents running NEO server.
type NEOSrv interface {
	ClusterName() string // name of the cluster
	MasterAddr()  string // address of the master

	Bugs() []string // list of known server bugs
}

// NEOPySrv represents running NEO/py server.
//
// Create it with StartNEOPySrv(XXX).
type NEOPySrv struct {
	pysrv       *xexec.Cmd    // spawned `runneo.py`
	workdir     string        // location for database and log files
	clusterName string        // name of the cluster
	opt         NEOPyOptions  // options for spawned server
	cancel      func()        // to stop pysrv
	done        chan struct{} // ready after Wait completes
	errExit     error         // error from Wait

	masterAddr string // address of master in spawned cluster
}

func (_ *NEOPySrv) Bugs() []string {
	return []string{
		// XXX
	}
}

type NEOPyOptions struct {
	// nmaster
	// npartition
	// nreplica

	// name
}

// StartNEOPySrv starts NEO/py server for clusterName NEO database located in workdir/.
// XXX dup wrt zeo?
func StartNEOPySrv(workdir, clusterName string, opt NEOPyOptions) (_ *NEOPySrv, err error) {
	defer xerr.Contextf(&err, "startneo %s/%s", workdir, clusterName)

	ctx, cancel := context.WithCancel(context.Background())

	readyf := workdir + "/ready"
	err = os.Remove(readyf)
	if os.IsNotExist(err) {
		err = nil
	}
	if err != nil {
		return nil, err
	}

	n := &NEOPySrv{workdir: workdir, clusterName: clusterName, cancel: cancel, done: make(chan struct{})}
	// XXX $PYTHONPATH to top, so that `import neo` works?
	n.pysrv = xexec.Command("./py/runneo.py", workdir, clusterName) // XXX +opt
	n.opt = opt
	// $TEMP -> workdir  (else NEO/py creates another one for e.g. coverage)
	n.pysrv.Env = append(os.Environ(), "TEMP="+workdir)
	n.pysrv.Stdin = nil
	n.pysrv.Stdout = os.Stdout
	n.pysrv.Stderr = os.Stderr
	err = n.pysrv.Start(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		n.errExit = n.pysrv.Wait()
		close(n.done)
	}()
	defer func() {
		if err != nil {
			n.Close()
		}
	}()

	// wait till spawned NEO is ready to serve clients
	for {
		select {
		default:
		case <-n.done:
			return nil, n.errExit
		}

		_, err := os.Stat(readyf)
		if err == nil {
			break // NEO cluster spawned by runneo.py is running
		}
		if os.IsNotExist(err) {
			err = nil // not yet
		}
		if err != nil {
			return nil, err
		}

		time.Sleep(100*time.Millisecond)
	}

	// retrieve master address
	masterAddr, err := ioutil.ReadFile(readyf)
	if err != nil {
		return nil, err
	}
	n.masterAddr = string(masterAddr)

	return n, nil
}

func (n *NEOPySrv) ClusterName() string {
	return n.clusterName
}

func (n *NEOPySrv) MasterAddr() string {
	return n.masterAddr
}

func (n *NEOPySrv) Close() (err error) {
	defer xerr.Contextf(&err, "stopneo %s", n.workdir)

	n.cancel()
	<-n.done
	err = n.errExit
	if _, ok := err.(*exec.ExitError); ok {
		err = nil // ignore exit status - it is always !0 on kill
	}
	return err
}

// ----------------

// tOptions represents options for testing.
// XXX dup in ZEO
type tOptions struct {
	Preload string // preload database with data from this location
}

// withNEOSrv tests f with all kind of NEO servers.
func withNEOSrv(t *testing.T, f func(t *testing.T, nsrv NEOSrv), optv ...tOptions) {
	t.Helper()

	opt := tOptions{}
	if len(optv) > 1 {
		panic("multiple tOptions not allowed")
	}
	if len(optv) == 1 {
		opt = optv[0]
	}

	// inWorkDir runs f under dedicated work directory.
	inWorkDir := func(t *testing.T, f func(workdir string)) {
		t.Helper()
		X := xtesting.FatalIf(t)
		work, err := ioutil.TempDir("", "neo"); X(err)
		defer os.RemoveAll(work)

		f(work)
	}

	// TODO + all variants with nreplic=X, npartition=Y, nmaster=Z, ... ?

	// NEO/py
	t.Run("py", func(t *testing.T) {
		t.Helper()
		// XXX needpy
		inWorkDir(t, func(workdir string) {
			X := xtesting.FatalIf(t)

			npy, err := StartNEOPySrv(workdir, "1", NEOPyOptions{}); X(err)
			defer func() {
				err := npy.Close(); X(err)
			}()

			if opt.Preload != "" {
				cmd := exec.Command("python", "-c",
					"from neo.scripts.neomigrate import main; main()",
					"-s", opt.Preload,
					"-d", npy.MasterAddr(), "-c", npy.ClusterName())
				cmd.Stdin  = nil
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				err := cmd.Run(); X(err)
			}

			f(t, npy)
		})
	})


	// TODO NEO/go
}

// withNEO tests f on all kinds of NEO servers connected to by NEO client.
func withNEO(t *testing.T, f func(t *testing.T, nsrv NEOSrv, ndrv *Client), optv ...tOptions) {
	t.Helper()
	withNEOSrv(t, func(t *testing.T, nsrv NEOSrv) {
		t.Helper()
		X := xtesting.FatalIf(t)
		ndrv, _, err := neoOpen(fmt.Sprintf("neo://%s@%s", nsrv.ClusterName(), nsrv.MasterAddr()),
					&zodb.DriverOptions{ReadOnly: true}); X(err)
		defer func() {
			err := ndrv.Close(); X(err)
		}()

		f(t, nsrv, ndrv)
	}, optv...)
}


// XXX TestHandshake ?

// XXX connect with wrong clusterName -> rejected

func TestEmptyDB(t *testing.T) {
	withNEO(t, func(t *testing.T, nsrv NEOSrv, n *Client) {
		xtesting.DrvTestEmptyDB(t, n)
	})
}

func TestLoad(t *testing.T) {
	X := xtesting.FatalIf(t)

	data := "../zodb/storage/fs1/testdata/1.fs"
	txnvOk, err := xtesting.LoadDBHistory(data); X(err)

	withNEO(t, func(t *testing.T, nsrv NEOSrv, n *Client) {
		xtesting.DrvTestLoad(t, n, txnvOk, nsrv.Bugs()...)
	}, tOptions{
		Preload: data,
	})
}

func TestWatch(t *testing.T) {
	t.Skip("FIXME currently hangs")
	withNEOSrv(t, func(t *testing.T, nsrv NEOSrv) {
		xtesting.DrvTestWatch(t, fmt.Sprintf("neo://%s@%s", nsrv.ClusterName(), nsrv.MasterAddr()), openClientByURL)
	})
}


func neoOpen(zurl string, opt *zodb.DriverOptions) (_ *Client, at0 zodb.Tid, err error) {
	defer xerr.Contextf(&err, "openneo %s", zurl)
	u, err := url.Parse(zurl)
	if err != nil {
		return nil, 0, err
	}

	n, at0, err := openClientByURL(context.Background(), u, opt)
	if err != nil {
		return nil, 0, err
	}

	return n.(*Client), at0, nil
}
