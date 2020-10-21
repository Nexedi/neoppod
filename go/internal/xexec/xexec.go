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

// Package xexec complements stdlib package os/exec.
// TODO move -> go123
package xexec

import (
	"context"
	"os/exec"
	"syscall"
)

// Cmd is similar to exec.Cmd and is created by Command.
type Cmd struct {
	*exec.Cmd

	done chan struct{}   // after wait completes
}

// Command is similar to exec.Command.
func Command(name string, argv ...string) *Cmd {
	cmd := &Cmd{
		Cmd:  exec.Command(name, argv...),
		done: make(chan struct{}),
	}
	return cmd
}


// XXX Cmd.CombinedOutput
// XXX Cmd.Output
// XXX Cmd.Run

// Start is similar to exec.Command.Start - it starts the specified command.
// Started command will be signalled with SIGTERM upon ctx cancel.
func (cmd *Cmd) Start(ctx context.Context) error {
	err := cmd.Cmd.Start()
	if err != nil {
		return err
	}

	// program started - propagate ctx.cancel -> SIGTERM
	go func() {
		select {
		case <-ctx.Done():
			cmd.Process.Signal(syscall.SIGTERM)	// XXX err

		case <-cmd.done:
			// ok
		}
	}()

	return nil
}

// Wait is the same as exec.Command.Wait.
func (cmd *Cmd) Wait() error {
	defer close(cmd.done)
	return cmd.Cmd.Wait()
}

// WaitOrKill it wait for spawned process to exit, but kills it with SIGKILL on killCtx cancel.
func (cmd *Cmd) WaitOrKill(ctxKill context.Context) error {
	// `kill -9` on ctx cancel
	go func() {
		select {
		case <-ctxKill.Done():
			_ = cmd.Process.Kill()

		case <-cmd.done:
			//ok
		}
	}()

	err := cmd.Wait()
	// return "context canceled" if we were forced to kill the child
	if ectx := ctxKill.Err(); ectx != nil {
		err = ectx
	}
	return err
}
