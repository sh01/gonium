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
   def __init__(self, ed=None, sc=None, aio=None, dtd=None, dnslm=None):
      if (ed is None):
         ed = ED_get()()
      self.ed = ed
      if (sc is None):
         sc = EMSignalCatcher(ed)
      self.sc = sc
      self.aio = aio
      self.dtd = dtd
      self.dnslm = dnslm
   
   def add_aio(self):
      """Instantiate and store EAIOManager (posix.aio)"""
      from .posix.aio import EAIOManager
      if not (self.aio is None):
         raise Exception('I already have an AIO object.')
      self.aio = EAIOManager(self)
   
   def add_dtd(self, wt_count=50):
      """Instantiate and store DataTransferDispatcher (posix.blockfd)"""
      if not (self.dtd is None):
         raise Exception('I already have a DTD object.')
      
      self.dtd = DataTransferDispatcher(wt_count)
      self.dtd.attach_ed(self.ed)
   
   def add_dnslm(self):
      """Instantiate and store a LookupManager (dns_resolving)"""
      if not (self.dnslm is None):
         raise Exception('I already have a DNSLM object.')
      
      rc = ResolverConfig.build_from_file()
      self.dnslm = rc.build_lookup_manager(self.ed)

# Ugly workaround for cyclical inter-file dependencies
from .posix.signal import EMSignalCatcher
from .posix.blockfd import DataTransferDispatcher
from .dns_resolving.stub import ResolverConfig

def _selftest():
   sa = ServiceAggregate()
   sa.add_aio()
   sa.add_dtd()
   sa.add_dnslm()

if (__name__ == '__main__'):
   _selftest()
