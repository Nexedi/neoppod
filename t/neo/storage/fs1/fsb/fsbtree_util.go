// DO NOT EDIT - AUTOGENERATED (by gen-fsbtree from github.com/cznic/b 93348d0)
// ---- 8< ----
package fsb
import (
	"bytes"
	"github.com/cznic/strutil"	// XXX better to not depend on it
)

func isNil(p interface{}) bool {
	switch x := p.(type) {
	case *x:
		if x == nil {
			return true
		}
	case *d:
		if x == nil {
			return true
		}
	}
	return false
}
func (t *Tree) Dump() string {
	var buf bytes.Buffer
	f := strutil.IndentFormatter(&buf, "\t")

	num := map[interface{}]int{}
	visited := map[interface{}]bool{}

	handle := func(p interface{}) int {
		if isNil(p) {
			return 0
		}

		if n, ok := num[p]; ok {
			return n
		}

		n := len(num) + 1
		num[p] = n
		return n
	}

	var pagedump func(interface{}, string)
	pagedump = func(p interface{}, pref string) {
		if isNil(p) || visited[p] {
			return
		}

		visited[p] = true
		switch x := p.(type) {
		case *x:
			h := handle(p)
			n := 0
			for i, v := range x.x {
				if v.ch != nil || v.k != 0 {
					n = i + 1
				}
			}
			f.Format("%sX#%d(%p) n %d:%d {", pref, h, x, x.c, n)
			a := []interface{}{}
			for i, v := range x.x[:n] {
				a = append(a, v.ch)
				if i != 0 {
					f.Format(" ")
				}
				f.Format("(C#%d K %v)", handle(v.ch), v.k)
			}
			f.Format("}\n")
			for _, p := range a {
				pagedump(p, pref+". ")
			}
		case *d:
			h := handle(p)
			n := 0
			for i, v := range x.d {
				if v.k != 0 || v.v != 0 {
					n = i + 1
				}
			}
			f.Format("%sD#%d(%p) P#%d N#%d n %d:%d {", pref, h, x, handle(x.p), handle(x.n), x.c, n)
			for i, v := range x.d[:n] {
				if i != 0 {
					f.Format(" ")
				}
				f.Format("%v:%v", v.k, v.v)
			}
			f.Format("}\n")
		}
	}

	pagedump(t.r, "")
	s := buf.String()
	if s != "" {
		s = s[:len(s)-1]
	}
	return s
}
