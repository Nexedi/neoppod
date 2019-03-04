// Copyright (C) 2017-2019  Nexedi SA and Contributors.
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

// Package pickletools provides utilities related to python pickles.
//
// It complements package ogórek (github.com/kisielk/og-rek).
//
// XXX dup in wcfs/internal/pycompat .
package pickletools

import (
	"encoding/binary"
	"fmt"
	"math/big"

	pickle "github.com/kisielk/og-rek"
)

// Xint64 tries to convert unpickled value to int64.
//
// (ogórek decodes python long as big.Int)
func Xint64(xv interface{}) (v int64, ok bool) {
	switch v := xv.(type) {
	case int64:
		return v, true
	case *big.Int:
		if v.IsInt64() {
			return v.Int64(), true
		}
	}

	return 0, false
}

// Xstrbytes verifies and extacts str|bytes from unpickled value.
func Xstrbytes(x interface{}) (string, error) {
	var s string
	switch x := x.(type) {
	default:
		return "", fmt.Errorf("expect str|bytes; got %T", x)

	case string:
		s = x

	case pickle.Bytes:
		s = string(x)
	}

	return s, nil
}

// Xstrbytes8 verifies and extracts [8](str|bytes) from unpickled value as big-endian u64.
func Xstrbytes8(x interface{}) (uint64, error) {
	s, err := Xstrbytes(x)
	if err != nil {
		return 0, err
	}

	if len(s) != 8 {
		return 0, fmt.Errorf("expect [8]bytes; got [%d]bytes", len(s))
	}

	return binary.BigEndian.Uint64([]byte(s)), nil
}