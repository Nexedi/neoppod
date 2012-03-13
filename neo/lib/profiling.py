#
# Copyright (C) 2010  Nexedi SA
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

"""
Profiling is done with tiny-profiler, a very simple profiler.

It is different from python's built-in profilers in that it requires
developpers to explicitely put probes on specific methods, reducing:
- profiling overhead
- undesired result entries

You can get this profiler at:
  https://svn.erp5.org/repos/public/erp5/trunk/utils/tiny_profiler
"""

PROFILING_ENABLED = False

if PROFILING_ENABLED:
    from tiny_profiler import profiler_decorator, profiler_report
else:
    def profiler_decorator(func):
        return func

    def profiler_report():
        pass
