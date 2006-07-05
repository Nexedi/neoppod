PYTHON_PY_FILES = __init__.py config.py neostorage.py mq.py
PYTHON_C_FILES = neoclient.c neoclient_wrap.c
ZOPE_DATA_FILES = component.xml

# XXX this should be configurable.
NEO_DIR = /usr/lib/zope/lib/python/NEO

EXTRA_DIST = $(PYTHON_PY_FILES) $(PYTHON_C_FILES) $(ZOPE_DATA_FILES)

PYTHON_EXTENSIONS = neoclient$(PYTHON_SO)
neoclient_OBJ_FILES = neoclient_wrap.$(OBJEXT) neoclient.$(OBJEXT)

BUILT_SOURCES = $(PYTHON_EXTENSIONS)
CLEANFILES = $(PYTHON_EXTENSIONS) $(neoclient_OBJ_FILES)

PYTHON_CCOMPILE = $(PYTHON_CC) $(PYTHON_CCSHARED) $(PYTHON_BASECFLAGS) \
	$(PYTHON_OPT) -Wall -W -I$(PYTHON_INCLUDE) -I$(top_srcdir)/include

neoclient$(PYTHON_SO): $(neoclient_OBJ_FILES) ../../lib/libneo.a
	$(PYTHON_LDSHARED) -o $@ $(neoclient_OBJ_FILES) -L../../lib -lneo

neoclient_wrap.$(OBJEXT): neoclient_wrap.c
	$(PYTHON_CCOMPILE) -c $< -o $@

neoclient.$(OBJEXT): neoclient.c
	$(PYTHON_CCOMPILE) -c $< -o $@

install-data-local: $(PYTHON_EXTENSIONS) $(PYTHON_PY_FILES) $(ZOPE_DATA_FILES)
	$(mkinstalldirs) $(DESTDIR)$(NEO_DIR)
	for file in $?; do $(INSTALL_DATA) $$file $(DESTDIR)$(NEO_DIR); done

uninstall-local:
	for file in $(PYTHON_EXTENSIONS) $(PYTHON_PY_FILES) $(ZOPE_DATA_FILES); do \
	  rm -f $(DESTDIR)$(NEO_DIR)/$$file; \
	done
