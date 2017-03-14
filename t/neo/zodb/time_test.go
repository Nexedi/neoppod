// TODO copyright / license
// TODO what it is

package zodb

import (
	"testing"
)

func TestTidTime(t *testing.T) {
	var testv = []struct {tid Tid; timeStr string} {
		{0x0000000000000000, "1900-01-01 00:00:00.000000"},
		{0x0285cbac258bf266, "1979-01-03 21:00:08.800000"},
		{0x037969f722a53488, "2008-10-24 05:11:08.120000"},
		{0x03b84285d71c57dd, "2016-07-01 09:41:50.416574"},
	}

	for _, tt := range testv {
		timeStr := tt.tid.Time().String()
		if timeStr != tt.timeStr {
			t.Errorf("%v: timeStr = %q  ; want %q", tt.tid, timeStr, tt.timeStr)
		}
	}
}
