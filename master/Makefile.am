MYSQL_LDFLAGS = @MYSQL_LDFLAGS@
MYSQL_INCLUDE = @MYSQL_INCLUDE@
MYSQL_LIBS = @MYSQL_LIBS@

AM_CFLAGS = -Wall -W 
AM_CPPFLAGS = -I${top_srcdir}/include ${MYSQL_INCLUDE}
LDADD = -lneo -lstdc++ ${MYSQL_LIBS}
AM_LDFLAGS = ${MYSQL_LDFLAGS} -L../lib

bin_PROGRAMS = neomaster

neomaster_SOURCES = master_return.c master_request.c master_node.c \
	master_return.h master_request.h 
