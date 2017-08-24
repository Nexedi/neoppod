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

package client
// base for intrusive list

// listHead is a list head entry for an element in an intrusive doubly-linked list.
//
// XXX doc how to get to container of this list head via unsafe.OffsetOf
//
// always call Init() to initialize a head before using it.
type listHead struct {
	// XXX needs to be created with .next = .prev = self
	next, prev *listHead
}

// Init initializes a head making it point to itself via .next and .prev
func (h *listHead) Init() {
	h.next = h
	h.prev = h
}

// Delete deletes h from its list
func (h *listHead) Delete() {
	h.next.prev = h.prev
	h.prev.next = h.next
	h.Init()
}

// MoveBefore moves a to be before b
// XXX ok to move if a was not previously on the list?
func (a *listHead) MoveBefore(b *listHead) {
	a.Delete()

	a.next = b
	b.prev = a
	a.prev = b.prev
	a.prev.next = a
}
