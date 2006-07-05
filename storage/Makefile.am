MYSQL_LDFLAGS = @MYSQL_LDFLAGS@
MYSQL_INCLUDE = @MYSQL_INCLUDE@
MYSQL_LIBS = @MYSQL_LIBS@

AM_CFLAGS = -Wall -W 
AM_CPPFLAGS = -I${top_srcdir}/include ${MYSQL_INCLUDE}
LDADD = -lneo -lstdc++ ${MYSQL_LIBS}
AM_LDFLAGS = ${MYSQL_LDFLAGS} -L../lib

bin_PROGRAMS = neostorage

neostorage_SOURCES = storage_return.c storage_request.c storage_node.c \
	storage_return.h storage_request.h
