// Copyright (C) 2017  Nexedi SA and Contributors.
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

// +build race

package race

import "unsafe"

/*
// symbols are e.g. in go/src/runtime/race/race_linux_amd64.syso
#cgo LDFLAGS: -Wl,--unresolved-symbols=ignore-in-object-files

// __tsan::ThreadIgnoreBegin(__tsan::ThreadState*, unsigned long)
// __tsan::ThreadIgnoreEnd(__tsan::ThreadState*, unsigned long)
extern void _ZN6__tsan17ThreadIgnoreBeginEPNS_11ThreadStateEm(void *, unsigned long);
extern void _ZN6__tsan15ThreadIgnoreEndEPNS_11ThreadStateEm(void *, unsigned long);
*/
import "C"

// Ways to tell race-detector to ignore "read/write" events from current thread.
// NOTE runtime.RaceDisable disables only "sync" part, not "read/write".

func IgnoreBegin(racectx uintptr) {
	C._ZN6__tsan17ThreadIgnoreBeginEPNS_11ThreadStateEm(unsafe.Pointer(racectx), 0)
}

func IgnoreEnd(racectx uintptr) {
	C._ZN6__tsan15ThreadIgnoreEndEPNS_11ThreadStateEm(unsafe.Pointer(racectx), 0)
}
