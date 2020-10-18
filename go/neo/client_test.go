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
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
	"time"

	"lab.nexedi.com/kirr/neo/go/internal/xtesting"

	"lab.nexedi.com/kirr/go123/xerr"
)

// NEOSrv represents running NEO server.
type NEOSrv interface {
	MasterAddr() string // address of the master
	// XXX +ClusterName
}

// NEOPySrv represents running NEO/py server.
//
// Create it with StartNEOPySrv(XXX).
type NEOPySrv struct {
	pysrv   *exec.Cmd	// spawned `XXX`
	workdir string		// location for database and log files
	opt     NEOPyOptions	// options for spawned server
	cancel  func()		// to stop pysrv
	done    chan struct{}	// ready after Wait completes
	errExit error		// error from Wait

	masterAddr string       // address of master in spawned cluster
}

// NEOPySrv.Bugs

type NEOPyOptions struct {
	// nmaster
	// npartition
	// nreplica

	// name
}

// StartNEOPySrv starts NEO/py server for NEO database located in workdir/.
// XXX dup wrt zeo?
func StartNEOPySrv(workdir string, opt NEOPyOptions) (_ *NEOPySrv, err error) {
	defer xerr.Contextf(&err, "startneo %s", workdir)

	ctx, cancel := context.WithCancel(context.Background())

	readyf := workdir + "/ready"
	err = os.Remove(readyf)
	if os.IsNotExist(err) {
		err = nil
	}
	if err != nil {
		return nil, err
	}

	n := &NEOPySrv{workdir: workdir, cancel: cancel, done: make(chan struct{})}
	n.pysrv = exec.CommandContext(ctx, "python", "./py/runneo.py", workdir) // XXX +opt
	n.opt = opt
	n.pysrv.Stdin = nil
	n.pysrv.Stdout = os.Stdout
	n.pysrv.Stderr = os.Stderr
	err = n.pysrv.Start()
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

// withNEOSrv tests f with all kind of NEO servers.
func withNEOSrv(t *testing.T, f func(t *testing.T, nsrv NEOSrv)) { // XXX +optv ?
	t.Helper()

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

			npy, err := StartNEOPySrv(workdir, NEOPyOptions{}); X(err)
			defer func() {
				err := npy.Close(); X(err)
			}()

			f(t, npy)
		})
	})


	// TODO NEO/go
}
