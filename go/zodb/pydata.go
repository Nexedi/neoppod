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

package zodb
// serialization compatibility with ZODB/py

import (
	"bytes"
	"fmt"

	pickle "github.com/kisielk/og-rek"
)

// PyData represents data stored into ZODB by Python applications.
//
// The format is based on python pickles. Basically every serialized object has
// two parts: class description and object state. See
//
//	https://github.com/zopefoundation/ZODB/blob/a89485c1/src/ZODB/serialize.py
//
// for format description.
type PyData []byte

// ClassName returns fully-qualified python class name used for object type.
//
// The format is "module.class".
// If pickle decoding fails - "?.?" is returned.
func (d PyData) ClassName() string {
	// see ObjectReader.getClassName & get_pickle_metadata in zodb/py
	p := pickle.NewDecoder(bytes.NewReader([]byte(d)))
	xklass, err := p.Decode()
	if err != nil {
		return "?.?"
	}

	if t, ok := xklass.(pickle.Tuple); ok {
		if len(t) != 2 {	// (klass, args)
			return "?.?"
		}
		xklass = t[0]
		if t, ok := xklass.(pickle.Tuple); ok {
			// py: "old style reference"
			if len(t) != 2 {
				return "?.?"	// (modname, classname)
			}
			return fmt.Sprintf("%s.%s", t...)
		}
	}

	if klass, ok := xklass.(pickle.Class); ok {
		return klass.Module + "." + klass.Name
	}

	return "?.?"
}
