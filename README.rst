NEO is a distributed, redundant and scalable implementation of ZODB API.
NEO stands for Nexedi Enterprise Object.

Overview
========

A NEO cluster is composed of the following types of nodes:

- "master" nodes (mandatory, 1 or more)

  Takes care of transactionality. Only one master node is really active
  (the active master node is called "primary master") at any given time,
  extra masters are spares (they are called "secondary masters").

- "storage" nodes (mandatory, 1 or more)

  Stores data, preserving history. All available storage nodes are in use
  simultaneously. This offers redundancy and data distribution.
  Available backends: MySQL (InnoDB, RocksDB or TokuDB), SQLite

- "admin" nodes (mandatory for startup, optional after)

  Accepts commands from neoctl tool and transmits them to the
  primary master, and monitors cluster state.

- "client" nodes

  Well... Something needing to store/load data in a NEO cluster.

ZODB API is fully implemented except:

- pack: only old revisions of objects are removed (it should be possible
  to use `zc.zodbdgc <https://pypi.python.org/pypi/zc.zodbdgc>`_
  for garbage collection)
- blobs: not implemented (not considered yet)

Any ZODB like FileStorage can be converted to NEO instantaneously,
which means the database is operational before all data are imported.
There's also a tool to convert back to FileStorage.

For more detailed information about features related to scalability,
see the `Architecture and Characteristics` section of https://neo.nexedi.com/.

Requirements
============

- Linux 2.6 or later

- Python 2.7.x (2.7.9 or later for SSL support)

- For storage nodes using MySQL backend:

  - MySQLdb: https://github.com/PyMySQL/mysqlclient-python

- For client nodes: ZODB 3.10.x or later

Installation
============

a. NEO can be installed like any other egg (see setup.py). Or you can simply
   make `neo` directory available for Python to import (for example, by
   adding its container directory to the PYTHONPATH environment variable).

b. Write a neo.conf file like the example provided. If you use MySQL,
   you'll also need create 1 database per storage node.

c. Start all required nodes::

    $ neomaster -f neo.conf
    $ neostorage -f neo.conf -s storage1
    $ neostorage -f neo.conf -s storage2
    $ neoadmin -f neo.conf

d. Tell the cluster to initialize storage nodes::

    $ neoctl -a <admin> start

e. Clients can connect when the cluster is in RUNNING state::

    $ neoctl -a <admin> print cluster
    RUNNING

f. See `importer.conf` file to import an existing database,
   or `neoctl` command for more administrative tasks.

Alternatively, you can use `neosimple` command to quickly setup a cluster for
testing.

How to use
==========

First make sure Python can import 'neo.client' package.

In zope
-------

a. Edit your zope.conf, add a neo import and edit the `zodb_db` section by
   replacing its filestorage subsection by a NEOStorage one.
   It should look like::

    %import neo.client
    <zodb_db main>
        <NEOStorage>
            master_nodes 127.0.0.1:10000
            name <cluster name>
        </NEOStorage>
        mount-point /
    </zodb_db>

b. Start zope

In a Python script
------------------

Just create the storage object and play with it::

  from neo.client.Storage import Storage
  s = Storage(master_nodes="127.0.0.1:10010", name="main")
  ...

"name" and "master_nodes" parameters have the same meaning as in
configuration file.

Shutting down
-------------

Before shutting down NEO, all clients like Zope instances should be stopped,
so that cluster become idle. This is required for multi-DB setups, to prevent
critical failures in second phase of TPC.

A cluster (i.e. masters+storages+admin) can be stopped gracefully by putting it
in STOPPING state using neoctl::

  neoctl -a <admin> set cluster STOPPING

This can also be done manually, which helps if your cluster is in bad state:

- Stop all master nodes first with a SIGINT or SIGTERM, so that storage nodes
  don't become in OUT_OF_DATE state.
- Next stop remaining nodes with a SIGINT or SIGTERM.

Master-slave asynchronous replication
-------------------------------------

This is the recommanded way to backup a NEO cluster.
Once a cluster with appropriate `upstream_cluster` & `upstream_masters`
configuration is started, you can switch it into backup mode
using::

  neoctl -a <admin> set cluster STARTING_BACKUP

It remembers it is in such mode when it is stopped, and it can be put back into
normal mode (RUNNING)  by setting it into STOPPING_BACKUP state.

Packs are currently not replicated, which means packing should always be done
up to a TID that is already fully replicated, so that the backup cluster has a
full history (and not random holes).

SSL support
-----------

In addition to any external solution like OpenVPN, NEO has builtin SSL support
to authenticate and encrypt communications between nodes.

All commands and configuration files have options to specify a CA certificate,
the node certificate and the node private key. A certificate can be shared
by several nodes.

NEO always uses the latest SSL protocol supported by the Python interpreter,
without fallback to older versions. A "SSL: WRONG_VERSION_NUMBER" error means
that a node runs in an older environment (Python + OpenSSL) than others.

Note also that you can't mix non-SSL nodes and SSL nodes, even between a
upstream cluster and a backup one. In doing so, connections can get stuck,
or fail with malformed packets or SSL handshake errors.

Deployment
==========

NEO has no built-in deployment features such as process daemonization. We use
`supervisor <http://supervisord.org/>`_ with configuration like below::

  [group:neo]
  programs=master_01,storage_01,admin

  [program:storage_01]
  priority=10
  command=neostorage -s storage_01 -f /neo/neo.conf

  [program:master_01]
  priority=20
  command=neomaster -s master_01 -f /neo/neo.conf

  [program:admin]
  priority=20
  command=neoadmin -s admin -f /neo/neo.conf

Developers
==========

Developers interested in NEO may refer to
`NEO Web site <https://neo.nexedi.com/>`_ and subscribe to following mailing
lists:

- `neo-users <https://mail.tiolive.com/mailman/listinfo/neo-users>`_:
  users discussion
- `neo-dev <https://mail.tiolive.com/mailman/listinfo/neo-dev>`_:
  developers discussion

Automated test results are published at
https://www.erp5.com/quality/integration/P-ERP5.Com.Unit%20Tests/Base_viewListMode?proxy_form_id=WebSection_viewERP5UnitTestForm&proxy_field_id=listbox&proxy_field_selection_name=WebSection_viewERP5UnitTestForm_listbox_selection&reset=1&listbox_title=NEO-%25

Commercial Support
==================

Nexedi provides commercial support for NEO: https://www.nexedi.com/
