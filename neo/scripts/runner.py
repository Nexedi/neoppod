#!/usr/bin/env python
#
# Copyright (C) 2009-2012  Nexedi SA
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

from neo.tests import getTempDirectory, __dict__ as neo_tests__dict__
from neo.tests.benchmark import BenchmarkRunner

# list of test modules
# each of them have to import its TestCase classes
UNIT_TEST_MODULES = [
    # generic parts
    'neo.tests.testBootstrap',
    'neo.tests.testConnection',
    'neo.tests.testEvent',
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
    'neo.tests.threaded.testReplication',
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


class NeoTestRunner(unittest.TestResult):
    """ Custom result class to build report with statistics per module """

    def __init__(self, title):
        unittest.TestResult.__init__(self)
        self._title = title
        self.modulesStats = {}
        self.failedImports = {}
        self.lastStart = None
        self.temp_directory = getTempDirectory()

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

    class ModuleStats(object):
        run = 0
        errors = 0
        success = 0
        failures = 0
        time = 0.0

    def _getModuleStats(self, test):
        module = test.__class__.__module__
        module = tuple(module.split('.'))
        try:
            return self.modulesStats[module]
        except KeyError:
            self.modulesStats[module] = self.ModuleStats()
            return self.modulesStats[module]

    def _updateTimer(self, stats):
        stats.time += time.time() - self.lastStart

    def startTest(self, test):
        unittest.TestResult.startTest(self, test)
        logging.info(" * TEST %s", test)
        stats = self._getModuleStats(test)
        stats.run += 1
        self.lastStart = time.time()

    def addSuccess(self, test):
        print "OK"
        unittest.TestResult.addSuccess(self, test)
        stats = self._getModuleStats(test)
        stats.success += 1
        self._updateTimer(stats)

    def addError(self, test, err):
        print "ERROR"
        unittest.TestResult.addError(self, test, err)
        stats = self._getModuleStats(test)
        stats.errors += 1
        self._updateTimer(stats)

    def addFailure(self, test, err):
        print "FAIL"
        unittest.TestResult.addFailure(self, test, err)
        stats = self._getModuleStats(test)
        stats.failures += 1
        self._updateTimer(stats)

    def _buildSummary(self, add_status):
        success = self.testsRun - len(self.errors) - len(self.failures)
        add_status('Directory', self.temp_directory)
        if self.testsRun:
            add_status('Status', '%.3f%%' % (success * 100.0 / self.testsRun))
        for var in os.environ.iterkeys():
            if var.startswith('NEO_TEST'):
                add_status(var, os.environ[var])
        # visual
        header       = "%25s |   run   | success |  errors |  fails  |   time   \n" % 'Test Module'
        separator    = "%25s-+---------+---------+---------+---------+----------\n" % ('-' * 25)
        format       = "%25s |   %3s   |   %3s   |   %3s   |   %3s   | %6.2fs   \n"
        group_f      = "%25s |         |         |         |         |          \n"
        # header
        s = ' ' * 30 + ' NEO TESTS REPORT'
        s += '\n'
        s += '\n' + header + separator
        group = None
        t_success = 0
        # for each test case
        for k, v in sorted(self.modulesStats.items()):
            # display group below its content
            _group = '.'.join(k[:-1])
            if group is None:
                group = _group
            if _group != group:
                s += separator + group_f % group + separator
                group = _group
            # test case stats
            t_success += v.success
            run, success = v.run or '.', v.success or '.'
            errors, failures = v.errors or '.', v.failures or '.'
            name = k[-1].lstrip('test')
            args = (name, run, success, errors, failures, v.time)
            s += format % args
        # the last group
        s += separator  + group_f % group + separator
        # the final summary
        errors, failures = len(self.errors) or '.', len(self.failures) or '.'
        args = ("Summary", self.testsRun, t_success, errors, failures, self.time)
        s += format % args + separator + '\n'
        return s

    def _buildErrors(self):
        s = ''
        test_formatter = lambda t: t.id()
        if len(self.errors):
            s += '\nERRORS:\n'
            for test, trace in self.errors:
                s += "%s\n" % test_formatter(test)
                s += "-------------------------------------------------------------\n"
                s += trace
                s += "-------------------------------------------------------------\n"
                s += '\n'
        if len(self.failures):
            s += '\nFAILURES:\n'
            for test, trace in self.failures:
                s += "%s\n" % test_formatter(test)
                s += "-------------------------------------------------------------\n"
                s += trace
                s += "-------------------------------------------------------------\n"
                s += '\n'
        return s

    def _buildWarnings(self):
        s = '\n'
        if self.failedImports:
            s += 'Failed imports :\n'
            for module, err in self.failedImports.items():
                s += '%s:\n%s' % (module, err)
        s += '\n'
        return s

    def buildReport(self, add_status):
        self.time = sum([s.time for s in self.modulesStats.values()])
        # TODO: Add 'Broken' for known failures (not a regression)
        #       and 'Fixed' for unexpected successes.
        self.subject = "%s Tests, %s Failed" % (
            self.testsRun, len(self.errors) + len(self.failures))
        summary = self._buildSummary(add_status)
        errors = self._buildErrors()
        warnings = self._buildWarnings()
        report = '\n'.join([summary, errors, warnings])
        return (self.subject, report)

class TestRunner(BenchmarkRunner):

    def add_options(self, parser):
        parser.add_option('-f', '--functional', action='store_true',
            help='Functional tests')
        parser.add_option('-u', '--unit', action='store_true',
            help='Unit & threaded tests')
        parser.add_option('-z', '--zodb', action='store_true',
            help='ZODB test suite running on a NEO')
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
        )

    def start(self):
        config = self._config
        # run requested tests
        runner = NeoTestRunner(
            title=config.title or 'Neo',
        )
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
