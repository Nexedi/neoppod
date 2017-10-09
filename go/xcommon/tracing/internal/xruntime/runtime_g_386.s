#include "textflag.h"

// +build 386

// func getg() *g
TEXT ·getg(SB),NOSPLIT,$0-8
	MOVL (TLS), AX
	MOVL AX, ret+0(FP)
	RET

