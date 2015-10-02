#!/usr/bin/env python
#
# Copyright (C) 2009-2015  Nexedi SA
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import traceback
import unittest
import logging
import time
import sys
import neo
import os
from collections import Counter, defaultdict
from cStringIO import StringIO
from unittest.runner import _WritelnDecorator
from neo.tests import getTempDirectory, __dict__ as neo_tests__dict__
from neo.tests.benchmark import BenchmarkRunner

# list of test modules
# each of them have to import its TestCase classes
UNIT_TEST_MODULES = [
    # generic parts
    'neo.tests.testBootstrap',
    'neo.tests.testConnection',
    'neo.tests.testHandler',
    'neo.tests.testNodes',
    'neo.tests.testDispatcher',
    'neo.tests.testUtil',
    'neo.tests.testPT',
    # master application
    'neo.tests.master.testClientHandler',
    'neo.tests.master.testElectionHandler',
    'neo.tests.master.testMasterApp',
    'neo.tests.master.testMasterPT',
    'neo.tests.master.testRecovery',
    'neo.tests.master.testStorageHandler',
    'neo.tests.master.testVerification',
    'neo.tests.master.testTransactions',
    # storage application
    'neo.tests.storage.testClientHandler',
    'neo.tests.storage.testInitializationHandler',
    'neo.tests.storage.testMasterHandler',
    'neo.tests.storage.testStorageApp',
    'neo.tests.storage.testStorage' + os.getenv('NEO_TESTS_ADAPTER', 'SQLite'),
    'neo.tests.storage.testVerificationHandler',
    'neo.tests.storage.testIdentificationHandler',
    'neo.tests.storage.testTransactions',
    # client application
    'neo.tests.client.testClientApp',
    'neo.tests.client.testMasterHandler',
    'neo.tests.client.testStorageHandler',
    'neo.tests.client.testConnectionPool',
    # light functional tests
    'neo.tests.threaded.test',
    'neo.tests.threaded.testImporter',
    'neo.tests.threaded.testReplication',
    'neo.tests.threaded.testSSL',
]

FUNC_TEST_MODULES = [
    'neo.tests.functional.testMaster',
    'neo.tests.functional.testClient',
    'neo.tests.functional.testCluster',
    'neo.tests.functional.testStorage',
]

ZODB_TEST_MODULES = [
    ('neo.tests.zodb.testBasic', 'check'),
    ('neo.tests.zodb.testConflict', 'check'),
    ('neo.tests.zodb.testHistory', 'check'),
    ('neo.tests.zodb.testIterator', 'check'),
    ('neo.tests.zodb.testMT', 'check'),
    ('neo.tests.zodb.testPack', 'check'),
    ('neo.tests.zodb.testPersistent', 'check'),
    ('neo.tests.zodb.testReadOnly', 'check'),
    ('neo.tests.zodb.testRevision', 'check'),
    #('neo.tests.zodb.testRecovery', 'check'),
    ('neo.tests.zodb.testSynchronization', 'check'),
    # ('neo.tests.zodb.testVersion', 'check'),
    ('neo.tests.zodb.testUndo', 'check'),
    ('neo.tests.zodb.testZODB', 'check'),
]


class NeoTestRunner(unittest.TextTestResult):
    """ Custom result class to build report with statistics per module """

    def __init__(self, title, verbosity):
        super(NeoTestRunner, self).__init__(
            _WritelnDecorator(sys.stderr), False, verbosity)
        self._title = title
        self.modulesStats = {}
        self.failedImports = {}
        self.run_dict = defaultdict(int)
        self.time_dict = defaultdict(int)
        self.temp_directory = getTempDirectory()

    def wasSuccessful(self):
        return not (self.failures or self.errors or self.unexpectedSuccesses)

    def run(self, name, modules):
        print '\n', name
        suite = unittest.TestSuite()
        loader = unittest.defaultTestLoader
        for test_module in modules:
            # load prefix if supplied
            if isinstance(test_module, tuple):
                test_module, prefix = test_module
                loader.testMethodPrefix = prefix
            else:
                loader.testMethodPrefix = 'test'
            try:
                test_module = __import__(test_module, globals(), locals(), ['*'])
            except ImportError, err:
                self.failedImports[test_module] = err
                print "Import of %s failed : %s" % (test_module, err)
                traceback.print_exc()
                continue
            suite.addTests(loader.loadTestsFromModule(test_module))
        suite.run(self)

    def startTest(self, test):
        super(NeoTestRunner, self).startTest(test)
        self.run_dict[test.__class__.__module__] += 1
        self.start_time = time.time()

    def stopTest(self, test):
        self.time_dict[test.__class__.__module__] += \
          time.time() - self.start_time
        super(NeoTestRunner, self).stopTest(test)

    def _buildSummary(self, add_status):
        unexpected_count = len(self.errors) + len(self.failures) \
                         + len(self.unexpectedSuccesses)
        expected_count = len(self.expectedFailures)
        success = self.testsRun - unexpected_count - expected_count
        add_status('Directory', self.temp_directory)
        if self.testsRun:
            add_status('Status', '%.3f%%' % (success * 100.0 / self.testsRun))
        for var in os.environ:
            if var.startswith('NEO_TEST'):
                add_status(var, os.environ[var])
        # visual
        header       = "%25s |  run  | unexpected | expected | skipped |  time    \n" % 'Test Module'
        separator    = "%25s-+-------+------------+----------+---------+----------\n" % ('-' * 25)
        format       = "%25s |  %3s  |     %3s    |    %3s   |   %3s   | %6.2fs   \n"
        group_f      = "%25s |       |            |          |         |          \n"
        # header
        s = ' ' * 30 + ' NEO TESTS REPORT\n\n' + header + separator
        group = None
        unexpected = Counter(x[0].__class__.__module__
                             for x in (self.errors, self.failures)
                             for x in x)
        unexpected.update(x.__class__.__module__
                          for x in self.unexpectedSuccesses)
        expected = Counter(x[0].__class__.__module__
                           for x in self.expectedFailures)
        skipped = Counter(x[0].__class__.__module__
                          for x in self.skipped)
        total_time = 0
        # for each test case
        for k, v in sorted(self.run_dict.iteritems()):
            # display group below its content
            _group, name = k.rsplit('.', 1)
            if _group != group:
                if group:
                    s += separator + group_f % group + separator
                group = _group
            t = self.time_dict[k]
            total_time += t
            s += format % (name.lstrip('test'), v, unexpected.get(k, '.'),
                           expected.get(k, '.'), skipped.get(k, '.'), t)
        # the last group
        s += separator  + group_f % group + separator
        # the final summary
        s += format % ("Summary", self.testsRun, unexpected_count or '.',
                       expected_count or '.', len(self.skipped) or '.',
                       total_time) + separator + '\n'
        return "%s Tests, %s Failed" % (self.testsRun, unexpected_count), s

    def buildReport(self, add_status):
        subject, summary = self._buildSummary(add_status)
        body = StringIO()
        body.write(summary)
        for test in self.unexpectedSuccesses:
            body.write("UNEXPECTED SUCCESS: %s\n" % self.getDescription(test))
        self.stream = _WritelnDecorator(body)
        self.printErrors()
        return subject, body.getvalue()

class TestRunner(BenchmarkRunner):

    def add_options(self, parser):
        parser.add_option('-f', '--functional', action='store_true',
            help='Functional tests')
        parser.add_option('-u', '--unit', action='store_true',
            help='Unit & threaded tests')
        parser.add_option('-z', '--zodb', action='store_true',
            help='ZODB test suite running on a NEO')
        parser.add_option('-v', '--verbose', action='store_true',
            help='Verbose output')
        parser.format_epilog = lambda _: """
Environment Variables:
  NEO_TESTS_ADAPTER           Default is SQLite for threaded clusters,
                              MySQL otherwise.

  MySQL specific:
    NEO_DB_PREFIX             default: %(DB_PREFIX)s
    NEO_DB_ADMIN              default: %(DB_ADMIN)s
    NEO_DB_PASSWD             default: %(DB_PASSWD)s
    NEO_DB_USER               default: %(DB_USER)s

  ZODB tests:
    NEO_TEST_ZODB_FUNCTIONAL  Clusters are threaded by default. If true,
                              they are built like in functional tests.
    NEO_TEST_ZODB_MASTERS     default: 1
    NEO_TEST_ZODB_PARTITIONS  default: 1
    NEO_TEST_ZODB_REPLICAS    default: 0
    NEO_TEST_ZODB_STORAGES    default: 1
""" % neo_tests__dict__

    def load_options(self, options, args):
        if not (options.unit or options.functional or options.zodb or args):
            sys.exit('Nothing to run, please give one of -f, -u, -z')
        return dict(
            unit = options.unit,
            functional = options.functional,
            zodb = options.zodb,
            verbosity = 2 if options.verbose else 1,
        )

    def start(self):
        config = self._config
        # run requested tests
        runner = NeoTestRunner(config.title or 'Neo', config.verbosity)
        try:
            if config.unit:
                runner.run('Unit tests', UNIT_TEST_MODULES)
            if config.functional:
                runner.run('Functional tests', FUNC_TEST_MODULES)
            if config.zodb:
                runner.run('ZODB tests', ZODB_TEST_MODULES)
        except KeyboardInterrupt:
            config['mail_to'] = None
            traceback.print_exc()
        # build report
        self._successful = runner.wasSuccessful()
        return runner.buildReport(self.add_status)

def main(args=None):
    runner = TestRunner()
    runner.run()
    return sys.exit(not runner.was_successful())

if __name__ == "__main__":
    main()
