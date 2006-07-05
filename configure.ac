dnl                                               -*- Autoconf -*-
dnl Process this file with autoconf to produce a configure script.

AC_PREREQ(2.5)
AC_INIT([Nexedi Enterprise Objects], [0.1], [aurel@nexedi.com], [NEO])
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([master/master_node.c])
AC_CONFIG_HEADER([config.h])

AC_ARG_WITH([mysql_config], 
            AS_HELP_STRING([--with-mysql_config=PATH],
                           [specify mysql_config]),
            [MYSQL_CONFIG=="$withval" ] 
)

dnl Checks for programs.
AC_PROG_CC
AC_PROG_RANLIB
AC_C_LONG_DOUBLE

AM_MAINTAINER_MODE

dnl Checks for libraries.
AC_CHECK_LIB([pthread], [pthread_mutex_init],,[AC_MSG_ERROR(libpthread not found)])
AC_CHECK_LIB([uuid], [uuid_parse],,[AC_MSG_ERROR(libuuid not found)])
AC_CHECK_LIB([z], [crc32],,[AC_MSG_ERROR(libz not found)])
AC_CHECK_LIB([ssl], [SSL_library_init],,[AC_MSG_ERROR(libssl not found)])

dnl Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([arpa/inet.h netdb.h netinet/in.h stdlib.h string.h sys/socket.h syslog.h unistd.h])

dnl Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_TYPE_SIZE_T
AC_STRUCT_TM

dnl Checks for library functions.
AC_FUNC_MALLOC
AC_TYPE_SIGNAL
AC_CHECK_FUNCS([bzero gethostbyname inet_ntoa memset socket stpcpy strerror strtol])

dnl check for python.
if test x = "x$PYTHON"; then
  AC_CHECK_PROGS([PYTHON], [python2 python])
  if test x = "x$PYTHON"; then
    AC_MSG_ERROR([cannot find python])
  fi
else
  AC_MSG_CHECKING([python])
  AC_CHECK_FILE([$PYTHON], , AC_MSG_ERROR([cannot find python]))
  AC_MSG_RESULT([$PYTHON])
fi

PYTHON_INCLUDE=`$PYTHON -c "from distutils import sysconfig; print sysconfig.get_python_inc()"`
AC_SUBST(PYTHON_INCLUDE)
PYTHON_OPT=`$PYTHON -c "from distutils import sysconfig; print sysconfig.get_config_var('OPT')"`
AC_SUBST(PYTHON_OPT)
PYTHON_BASECFLAGS=`$PYTHON -c "from distutils import sysconfig; print sysconfig.get_config_var('BASECFLAGS')"`
AC_SUBST(PYTHON_BASECFLAGS)
PYTHON_CCSHARED=`$PYTHON -c "from distutils import sysconfig; print sysconfig.get_config_var('CCSHARED')"`
AC_SUBST(PYTHON_CCSHARED)
PYTHON_LDSHARED=`$PYTHON -c "from distutils import sysconfig; print sysconfig.get_config_var('LDSHARED')"`
AC_SUBST(PYTHON_LDSHARED)
PYTHON_SO=`$PYTHON -c "from distutils import sysconfig; print sysconfig.get_config_var('SO')"`
AC_SUBST(PYTHON_SO)
PYTHON_CC=`$PYTHON -c "from distutils import sysconfig; print sysconfig.get_config_var('CC')"`
AC_SUBST(PYTHON_CC)

dnl check for mysql_config.
if test x = "x$MYSQL_CONFIG"; then
  AC_CHECK_PROGS([MYSQL_CONFIG], [mysql_config])
  if test x = "x$MYSQL_CONFIG"; then
    AC_MSG_ERROR([cannot find mysql_config])
  fi
else
  AC_MSG_CHECKING([mysql_config])
  AC_CHECK_FILE([$MYSQL_CONFIG], , AC_MSG_ERROR([cannot find mysql_config]))
  AC_MSG_RESULT([$MYSQL_CONFIG])
fi

MYSQL_LDFLAGS="" # XXX mysql_config does not separate -L.
MYSQL_INCLUDE=`$MYSQL_CONFIG --include`
MYSQL_LIBS=`$MYSQL_CONFIG --libmysqld-libs`
AC_SUBST(MYSQL_LDFLAGS)
AC_SUBST(MYSQL_INCLUDE)
AC_SUBST(MYSQL_LIBS)

AC_CONFIG_FILES([Makefile master/Makefile storage/Makefile 
                include/Makefile lib/Makefile doc/Makefile
                test/Makefile client/Makefile client/NEO/Makefile])

AC_OUTPUT
