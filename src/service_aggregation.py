#!/usr/bin/env python
#Copyright 2008 Sebastian Hagen
# This file is part of gonium.
#
# gonium is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# gonium is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from .fdm import ED_get


class ServiceAggregate:
   """Aggregate of pseudo-singleton highly-stateful callbacking services"""
   def __init__(self, ed=None, sc=None, aio=None):
      if (ed is None):
         ed = ED_get()()
      if (sc is None):
         sc = EMSignalCatcher(ed)
      if (aio is None):
         aio = EAIOManager(self)
      self.ed = ed
      self.sc = sc
      self.aio = aio

# Ugly workaround for cyclical inter-file dependencies
from .posix.signal import EMSignalCatcher
from .posix.aio import EAIOManager


def _selftest():
   ServiceAggregate()

if (__name__ == '__main__'):
   _selftest()
