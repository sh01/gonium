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
import signal

from collections import deque
from ..service_aggregation import ServiceAggregate
from .signal import SA_RESTART
from . import _aio
from ._aio import LIO_READ, LIO_WRITE, AIOManager, AIORequest

_logger = logging.getLogger('gonium.posix.aio')
_log = _logger.log


class EAIORequest(AIORequest):
   def __new__(cls, *args, callback):
      return AIORequest.__new__(cls, *args)
   
   def __init__(self, mode, buf, *args, callback):
      AIORequest.__init__(mode, buf, *args)
      self.buf = buf
      self.callback = callback
   
   def __repr__(self):
      return ('<AIORequest at {}, mode {!a}, fd {!a}:{!a}, memory {!a}({} bytes)>'
              ''.format(id(self), self.mode, self.fd, self.offset, self.buf,
              len(self.buf)))


class EAIOManager(AIOManager):
   """AIOManager subclass with eventing support"""
   REQ_CLS = EAIORequest
   MODE_READ = LIO_READ
   MODE_WRITE = LIO_WRITE
   
   AIO_SIGNAL = signal.SIGIO
   def __init__(self, sa:ServiceAggregate, *args, **kwargs):
      sa.sc.sighandler_install(self.AIO_SIGNAL, SA_RESTART)
      self._listeners = (
         sa.sc.handle_signals.new_listener(self._handle_signals),
         sa.sc.handle_overflow.new_listener(self._handle_overflow)
      )
      AIOManager.__init__(self, *args, **kwargs)
   
   def _handle_signals(self, si_l:collections.abc.Sequence):
      """Deal with signals indicating AIO completion"""
      sigvals = [si.value_int for si in si_l if (si.signo == self.AIO_SIGNAL)]
      if (not sigvals):
         return
      self._process_finished_requests(self.get_results(sigvals))
      self._handle_overflow()
      
   def _process_finished_requests(self, req_s):
      for req in req_s:
         if (req is None):
            # Signal / suspend() race condition result: We've already processed
            # this
            continue
         try:
            req.callback(req)
         except Exception:
            if (req.callback is None):
               continue
            _log(40, 'Exception in AIO handler {} on Request {}:'.format(req.callback, req), exc_info=True)
   
   def _handle_overflow(self):
      """Deal with lost signals"""
      self._process_finished_requests(self.suspend(0))
   
   def io(self, req_s:collections.abc.Sequence):
      """Request IO action
      
      req_s: Sequence of EAIORequest objects
      """
      for req in req_s:
         AIOManager.io(self,req)

def _selftest():
   _test_aiom(EAIOManager)

#1024, 4096
def _test_aiom(aiom_cls, test_count=1024, chunksize=4096):
   import os
   import struct
   from ..fdm import ED_get
   from .signal import EMSignalCatcher, SA_RESTART
   from .._debugging import streamlogger_setup; streamlogger_setup()
   from ..service_aggregation import ServiceAggregate
   
   sa = ServiceAggregate(aio=False)
   ed = sa.ed
   sc = sa.sc
   aio_m = aiom_cls(sa)
   
   baseflags = os.O_RDWR
   
   fn = b'__gonium_aio.test.tmp'
   
   print('== Setup ==')
   
   pending_events = deque()
   def handle_signals(si_l):
      for si in si_l:
         if (si.signo != signal.SIGIO):
            print('Got signal {0}; what am I supposed to do with that?'.format(si.signo))
            continue
         pending_events.append(si.value_int)
   
   sc.sighandler_install(signal.SIGIO, SA_RESTART)
   
   def ofhandler():
      print('-- Got signal queue overflow.')
   
   def aio_wres_process(req):
      nonlocal fail_count, ev_count
      if (req.rc > 0):
         ev_count += 1
         if (ev_count == test_count):
            ed.shutdown()
         return
      print('req {} failed with rc {}.'.format(req, req.rc))
      fail_count += 1
   
   def aio_rres_process(req):
      nonlocal fail_count, ev_count
      (i,) = struct.unpack('>L', req.buf[:4])
      if (req.offset == i*chunksize):
         ev_count += 1
         if (ev_count == test_count):
            ed.shutdown()
         return
      print('FAIL: Req at offset {}; read: {}'.format(req.offset, req.buf))
      fail_count += 1
   
   def aio_phase2():
      i = 0
      while (True):
         data = os.read(f, chunksize)
         if (len(data) == 0):
            break
         if (len(data) < chunksize):
            raise ValueError('Last block blockingly read contained {} bytes: {}.'.format(len(data), data))
         (j,) = struct.unpack('>L', data[:4])
         if (j == i):
            i += 1
            continue
         raise ValueError('Read bytes {} from index {}.'.format(data, i*chunksize))
   
   def results_check():
      if (fail_count):
         raise Exception("{} of {} of {} tests failed.".format(fail_count, ev_count, test_count))
      if (ev_count != test_count):
         raise Exception("Only got results for {} of {} tests (no confirmed failures).".format(ev_count, test_count))
      print('{} of {} tests succeeded; no confirmed failures.'.format(ev_count, test_count))
   
   f = os.open(fn,baseflags|os.O_CREAT)
   
   fail_count = 0
   ev_count = 0
   sc.handle_overflow.new_listener(ofhandler)
   sc.handle_signals.new_listener(handle_signals)
   
   write_cmds = deque()
   for i in range(test_count):
      buf = bytearray(chunksize)
      buf[:4] = struct.pack('>L', i)
      write_cmds.append(aio_m.REQ_CLS(aio_m.MODE_WRITE, buf, f, i*chunksize, callback=aio_wres_process))
   
   
   print('== Write test ==')
   aio_m.io(write_cmds)
   write_cmds.clear()
   #ed.set_timer(900, ed.shutdown)
   ed.event_loop()
   os.close(f)
   f = os.open(fn,baseflags)
   results_check()
   os.close(f)
   f = os.open(fn,baseflags)
   print('== Checking written data ==')
   aio_phase2()
   print('...no errors detected.')
   print('== Read test ==')
   
   class AIORS(AIORequest):
      pass
   
   read_cmds = deque()
   for i in range(test_count):
      buf = bytearray(chunksize)
      read_cmds.append(aio_m.REQ_CLS(aio_m.MODE_READ, buf, f, i*chunksize, callback=aio_rres_process))
   aio_m.io(read_cmds)
   
   fail_count = 0
   ev_count = 0
   ed.event_loop()
   results_check()
   
   os.close(f)
   os.remove(fn)


if (__name__ == '__main__'):
   _selftest()
