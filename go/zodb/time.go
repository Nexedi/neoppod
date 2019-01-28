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
	// round to microsecond on formatting: zodb/py does this, and without rounding it is sometimes
	// not exactly bit-to-bit the same in text output compared to zodb/py. Example:
	// 037969f722a53488: timeStr = "2008-10-24 05:11:08.119999"  ; want "2008-10-24 05:11:08.120000"
	//
	// This happens because Go's time.Format() does not perform rounding,
	// and so even if t = 0.999, formatting it with .00 won't give 0.10.
	//
	// NOTE UTC() in case we get TimeStamp with modified from-outside location.
	tµs := t.UTC().Round(time.Microsecond)

	return tµs.AppendFormat(b, "2006-01-02 15:04:05.000000")
}


// Time converts tid to time.
func (tid Tid) Time() TimeStamp {
	// the same as _parseRaw in TimeStamp/py
	// https://github.com/zopefoundation/persistent/blob/8c645429/persistent/timestamp.py#L72
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

	return TimeStamp{t}
}

// TidFromTime converts time to tid.
func TidFromTime(t time.Time) Tid {
	t = t.UTC()

	// the same as _makeRaw in TimeStamp/py
	// https://github.com/zopefoundation/persistent/blob/8c645429/persistent/timestamp.py#L66
	year  := uint64(t.Year())
	month := uint64(t.Month())
	day   := uint64(t.Day())
	hour  := uint64(t.Hour())
	min   := uint64(t.Minute())
	sec   := uint64(t.Second())
	nsec  := uint64(t.Nanosecond())

	a := ((year - 1900)*12 + month - 1) * 31 + day - 1
	a  = (a * 24 + hour) * 60 + min

	// for seconds/nseconds: use 2 extra bits of precision to be able to
	// round after / 1E9 and / 60 divisions. We are safe to use 2 bits,
	// since 10^9 ≤ 2^30 and it all fits into uint32. However for b for
	// intermediate values we are free to use whole uint64, so x can be >
	// than 2 too.
	const x = 2
	b := sec << (32+x)
	b += (nsec << (32+x)) / 1E9
	b /= 60
	roundup := (b & ((1<<x)-1)) >= (1<<(x-1))
	b /= (1<<x)
	if roundup {
		b += 1
	}

	return Tid((a << 32) | b)
}

// δtid returns distance from tid1 to tid2 in term of time.
//
// it can be thought as (tid2 - tid1).
func δtid(tid1, tid2 Tid) time.Duration {
	d := tid2.Time().Sub(tid1.Time().Time)
	return d
}

// tabs returns abs value of time.Duration .
func tabs(δt time.Duration) time.Duration {
	if δt < 0 {
		δt = -δt
	}
	return δt
}
