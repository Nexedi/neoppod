package pkg2

import "a/pkg1" // TODO should not be needed eventually

//trace:import a/pkg1


//trace:event DoSomething(i, j int, q string)

func DoSomething(i, j int, q string) {
	traceDoSomething(i, j, q)
}
