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

import collections
import logging
import os

from ._io import IORequest, IOManager, IO_CMD_PREAD, IO_CMD_PWRITE
from ..service_aggregation import ServiceAggregate

_logger = logging.getLogger()
_log = _logger.log

class LinuxAIORequest(IORequest):
   """Linux AIO Request."""
   def __new__(cls, *args, callback):
      return IORequest.__new__(cls, *args)
   
   def __init__(self, mode, buf, *args, callback):
      IORequest.__init__(self)
      self.buf = buf
      self.callback = callback


class LinuxAIOManager(IOManager):
   """Linux AIOManager with eventing support"""
   REQ_CLS = LinuxAIORequest
   MODE_READ = IO_CMD_PREAD
   MODE_WRITE = IO_CMD_PWRITE
   def __new__(cls, sa:ServiceAggregate, size=12800):
      return IOManager.__new__(cls, size)
   
   def __init__(self, sa, *args, **kwargs):
      self._fdwrap = sa.ed.fd_wrap(self.fd)
      self._fdwrap.process_readability = self._process_readability
      self._fdwrap.read_r()
   
   def _process_readability(self):
      req_s = self.getevents(0,0)
      try:
         os.read(self.fd, 1024)
      except OSError:
         pass
      
      for req in req_s:
         try:
            req.callback(req)
         except Exception:
            if (req.callback is None):
               continue
            _log(40, 'Exception in AIO handler {0} on Request {1}:'.format(req.callback, req), exc_info=True)
   
   def io(self, req_s:collections.abc.Sequence):
      """Request IO action
      
      req_s: Sequence of LinuxAIORequest objects
      """
      self.submit(req_s)


def _selftest():
   from ..posix.aio import _test_aiom
   _test_aiom(LinuxAIOManager)

if (__name__ == '__main__'):
   _selftest()
