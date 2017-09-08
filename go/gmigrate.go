// gmigrate - show number of times G migrates to another M (OS thread).
// usage: `go tool trace -d <trace.out> |gmigrate`
package main

// +build ignore

import (
	"bufio"
	"errors"
	"io"
	"log"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// we analyze input stream and take notices on ProcStart and GoStart events.
// when a ProcStart event comes, e.g.
//
//	9782014 ProcStart p=2 g=0 off=133138 thread=5
//
// we remember that a P is currently running on an M (=thread).
//
// when a GoStart event comes, e.g.
//
//	9782310 GoStart p=2 g=33 off=133142 g=33 seq=0
//
// we see that a G is using P to run, and by using previously noted P->M
// relation, conclude G->M relation.
//
// Then for every G noticed we record how much it changes its M.

type procStart struct {
	p int
	m int // =thread
}

type goStart struct {
	g int
	p int
}

// information about a G
type gInfo struct {
	g	 int
	m        int // last time was running on this M
	nmigrate int // how much times migrated between different Ms
}

func main() {
	var pm = map[int]int{}		// p -> m
	var gg = map[int]*gInfo{}	// g -> (m, #migrate)

	in := bufio.NewReader(os.Stdin)

	tstart, tend, tprev := -1, -1, -1
	for lineno := 1;; lineno++{
		bad := func(err error) {
			log.Fatalf("%d: %v", lineno, err)
		}
		badf := func(format string, argv ...interface{}) {
			bad(fmt.Errorf(format, argv...))
		}

		l, err := in.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			bad(err)
		}
		l = l[:len(l)-1] // strip trailing '\n'

		t, evname, args, err := parseLineHeader(l)
		if err != nil {
			bad(err)
		}

		if t < tprev {
			badf("time monotonity broken")
		}
		tprev = t

		if tstart == -1 {
			tstart = t
		} else {
			tend = t
		}

		switch evname {
		case "ProcStart":
			pstart, err := parseProcStart(args)
			if err != nil {
				bad(err)
			}

			pm[pstart.p] = pstart.m

		case "GoStart":
			gstart, err := parseGoStart(args)
			if err != nil {
				bad(err)
			}

			m, ok := pm[gstart.p]
			if !ok {
				badf("G%d start on P%d, but no matching previous P%d start",
					gstart.g, gstart.p, gstart.p)
			}

			g, seen := gg[gstart.g]
			if !seen {
				gg[gstart.g] = &gInfo{g: gstart.g, m: m, nmigrate: 0}
				break
			}

			if g.m != m {
				g.m = m
				g.nmigrate++
			}
		}
	}

	// all information collected - analyze
	gv := make([]*gInfo, 0, len(gg))
	for _, g := range gg {
		gv = append(gv, g)
	}

	// order: (nmigrate, g)↓
	sort.Slice(gv, func(i, j int) bool {
		ni, nj := gv[i].nmigrate, gv[j].nmigrate
		return (nj < ni) || (nj == ni && gv[j].g < gv[i].g) // reverse
	})

	fmt.Printf("G\tN(migrate)\tmigrate/s\n")
	fmt.Printf("-\t----------\t---------\n")
	for _, g := range gv {
		fmt.Printf("G%d\t%8d\t%8.1f\n", g.g, g.nmigrate,
			float64(g.nmigrate) / (float64(tend - tstart) * 1E-9))
	}
}

// "9782014 ProcStart p=2 g=0 off=133138 thread=5"
// ->
// 9782014 "ProcStart" "p=2 g=0 off=133138 thread=5"
func parseLineHeader(l string) (t int, event, args string, err error) {
	sp := strings.IndexByte(l, ' ')
	if sp < 0 {
		return 0, "", "", fmt.Errorf("parse: invalid timestamp")
	}
	t, err = strconv.Atoi(l[:sp])
	if err != nil {
		return 0, "", "", fmt.Errorf("parse: invalid timestamp")
	}

	l = l[sp+1:]
	sp = strings.IndexByte(l, ' ')
	if sp < 0 {
		return 0, "", "", fmt.Errorf("parse: invalid event name")
	}

	return t, l[:sp], l[sp+1:], nil
}

// ex: 9782014 ProcStart p=2 g=0 off=133138 thread=5
var (
	pStartArgvRe = regexp.MustCompile("^p=([^ ]+) g=[^ ]+ off=[^ ]+ thread=([^ ]+)$")
	pStartArgvErr = errors.New("ProcStart: argv invalid")
)

func parseProcStart(args string) (procStart, error) {
	argv := pStartArgvRe.FindStringSubmatch(args)
	if argv == nil {
		return procStart{}, pStartArgvErr
	}

	var pstart procStart
	var err error
	pstart.p, err = strconv.Atoi(argv[1])
	if err != nil {
		return procStart{}, pStartArgvErr
	}

	pstart.m, err = strconv.Atoi(argv[2])
	if err != nil {
		return procStart{}, pStartArgvErr
	}

	return pstart, nil
}

// ex: 9782310 GoStart p=2 g=33 off=133142 g=33 seq=0
var (
	gStartArgvRe = regexp.MustCompile("^p=([^ ]+) g=([^ ]+) off=[^ ]+ g=([^ ]+) seq=[^ ]+$")
	gStartArgvErr = errors.New("GoStart: argv invalid")
)

func parseGoStart(args string) (goStart, error) {
	argv := gStartArgvRe.FindStringSubmatch(args)
	if argv == nil {
		return goStart{}, gStartArgvErr
	}

	var gstart goStart
	var err error
	gstart.p, err = strconv.Atoi(argv[1])
	if err != nil {
		return goStart{}, gStartArgvErr
	}

	gstart.g, err = strconv.Atoi(argv[2])
	if err != nil {
		return goStart{}, gStartArgvErr
	}

	// double-check g is the same
	g2, err := strconv.Atoi(argv[3])
	if g2 != gstart.g {
		log.Print("found GoStart with different g")
		return goStart{}, gStartArgvErr
	}

	return gstart, nil
}
