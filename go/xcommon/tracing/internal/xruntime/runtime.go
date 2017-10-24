// Copyright (C) 2016-2017  Nexedi SA and Contributors.
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

package xruntime
// stop-the-world that should probably be in public xruntime, but I'm (yet)
// hesitating to expose the API to public.

import _ "unsafe"

//go:linkname runtime_stopTheWorld runtime.stopTheWorld
//go:linkname runtime_startTheWorld runtime.startTheWorld

func runtime_stopTheWorld(reason string)
func runtime_startTheWorld()

// StopTheWorld returns with the world stopped.
//
// Current goroutine remains the only one who is running, with others
// goroutines stopped at safe GC points.
// It requires careful programming as many things that normally work lead to
// fatal errors when the world is stopped - for example using timers would be
// invalid, but adjusting plain values in memory is ok.
func StopTheWorld(reason string) {
	runtime_stopTheWorld(reason)
}

// StartTheWorld restarts the world after it was stopped by StopTheWorld.
func StartTheWorld() {
	runtime_startTheWorld()
}
