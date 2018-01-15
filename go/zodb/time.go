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

package zodb
// tid connection with time

import (
	"time"
)

// TimeStamp is the same as time.Time only .String() is adjusted to be the same as in ZODB/py.
//
// XXX get rid eventually of this and just use time.Time.
type TimeStamp struct {
	time.Time
}

func (t TimeStamp) String() string {
	return string(t.XFmtString(nil))
}

func (t TimeStamp) XFmtString(b []byte) []byte {
	// NOTE UTC() in case we get TimeStamp with modified from-outside location
	return t.UTC().AppendFormat(b, "2006-01-02 15:04:05.000000")
}


// Time converts tid to time
func (tid Tid) Time() TimeStamp {
	// the same as _parseRaw in TimeStamp/py
	// https://github.com/zopefoundation/persistent/blob/aba23595/persistent/timestamp.py#L75
	a := uint64(tid) >> 32
	b := uint64(tid) & (1 << 32 - 1)
	min   := a % 60
	hour  := a / 60 % 24
	day   := a / (60 * 24) % 31 + 1
	month := a / (60 * 24 * 31) % 12 + 1
	year  := a / (60 * 24 * 31 * 12) + 1900
	sec   := b * 60 / (1 << 32)
	nsec  := (b * 60 - (sec << 32)) * 1E9 / (1 << 32)

	t := time.Date(
		int(year),
		time.Month(month),
		int(day),
		int(hour),
		int(min),
		int(sec),
		int(nsec),
		time.UTC)

	// round to microsecond: zodb/py does this, and without rounding it is sometimes
	// not exactly bit-to-bit the same in text output compared to zodb/py. Example:
	// 037969f722a53488: timeStr = "2008-10-24 05:11:08.119999"  ; want "2008-10-24 05:11:08.120000"
	t = t.Round(time.Microsecond)

	return TimeStamp{t}
}


// TODO TidFromTime()
// TODO TidFromTimeStamp()
// TODO TidForNow() ?
