package pkg1

/*
#include <stdio.h>

void hello() {
	printf("Hello World (from C)\n");
}
*/
import "C"

// trace event defined in a cgo file
//trace:event traceHello()

func Hello() {
	traceHello()
	C.hello()
}
