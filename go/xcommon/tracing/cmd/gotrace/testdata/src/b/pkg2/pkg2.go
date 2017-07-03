package pkg2

// XXX vvv kill
//import "a/pkg1" // TODO should not be needed eventually

//trace:import "a/pkg1"

// additional tracepoint which pkg2 defines
//trace:event traceDoSomething(i, j int, q string)

func DoSomething(i, j int, q string) {
	traceDoSomething(i, j, q)
}
