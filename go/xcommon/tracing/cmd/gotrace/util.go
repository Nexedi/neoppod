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

package main
// misc utilities

import (
	"bytes"
	"fmt"
	"sort"
)

// Buffer is bytes.Buffer + syntatic sugar
type Buffer struct {
	bytes.Buffer
}

func (b *Buffer) emit(format string, argv ...interface{}) {
	fmt.Fprintf(b, format+"\n", argv...)
}


// StrSet is set<string>
type StrSet map[string]struct{}

func (s StrSet) Add(itemv ...string) {
	for _, item := range itemv {
		s[item] = struct{}{}
	}
}

func (s StrSet) Delete(item string) {
	delete(s, item)
}

func (s StrSet) Has(item string) bool {
	_, has := s[item]
	return has
}

// Itemv returns ordered slice of set items
func (s StrSet) Itemv() []string {
	itemv := make([]string, 0, len(s))
	for item := range s {
		itemv = append(itemv, item)
	}
	sort.Strings(itemv)
	return itemv
}
