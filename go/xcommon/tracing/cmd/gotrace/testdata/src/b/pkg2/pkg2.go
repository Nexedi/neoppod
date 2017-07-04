package pkg2

// trace-import another package
// NOTE "a/pkg1" is not regularly imported
////trace:import "a/pkg1"

// XXX temp
//trace:import bbb "a/pkg1"

// additional tracepoint which pkg2 defines
//trace:event traceDoSomething(i, j int, q string)

func DoSomething(i, j int, q string) {
	traceDoSomething(i, j, q)
}
