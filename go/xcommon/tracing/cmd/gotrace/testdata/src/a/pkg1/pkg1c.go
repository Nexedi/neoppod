package pkg1

/*
#include <stdio.h>

void hello() {
	printf("Hello World (from C)\n");
}
*/
import "C"

// FIXME vvv does not currently work because go/loader gives us already
// preprocessed result without original comments.
//
// trace event defined in a cgo file
//trace:event traceHello()

func Hello() {
	traceHello()
	C.hello()
}
