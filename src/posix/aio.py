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

from ..posix import signal as signal_
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
      return ('<AIORequest at {0}, mode {1}, fd {2}:{3}, memory {4}({5} bytes)>'
              ''.format(id(self), self.mode, self.fd, self.offset, self.buf,
              len(self.buf)))


class EAIOManager(AIOManager):
   """AIOManager subclass with eventing support"""
   AIO_SIGNAL = signal.SIGIO
   def __init__(self, sc:signal_.EMSignalCatcher, *args, **kwargs):
      sc.sighandler_install(self.AIO_SIGNAL, SA_RESTART)
      self._listeners = (
         sc.handle_signals.new_listener(self._handle_signals),
         sc.handle_overflow.new_listener(self._handle_overflow)
      )
      AIOManager.__init__(self, *args, **kwargs)
   
   def _handle_signals(self, si_l:collections.Sequence):
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
            _log(40, 'Exception in AIO handler {0} on Request {1}:'.format(req.callback, req), exc_info=True)
   
   def _handle_overflow(self):
      """Deal with lost signals"""
      self._process_finished_requests(self.suspend(0))
   
   def io(self, req_s:collections.Sequence):
      """Request IO action
      
      mode: Either LIO_WRITE or LIO_READ
      buf: buffer to read from / write to
      filelike: filelike or fd to write to / read from
      offset: offset on filelike at which to start IO
      callback: object to call when this is finished
      """
      for req in req_s:
         AIOManager.io(self,req)
      

def _selftest():
   import struct
   from ..fdm import ED_get
   from .signal import EMSignalCatcher, SA_RESTART
   from .._debugging import streamlogger_setup; streamlogger_setup()
   
   ed = ED_get()()
   sc = EMSignalCatcher(ed)
   aio_m = EAIOManager(sc)
   TEST_COUNT = 1024
   CHUNKSIZE = 40960
   
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
         if (ev_count == TEST_COUNT):
            ed.shutdown()
         return
      print('req {0} failed with rc {1}.'.format(req, req.rc))
      fail_count += 1
   
   def aio_rres_process(req):
      nonlocal fail_count, ev_count
      (i,) = struct.unpack('>L', req.buf[:4])
      if (req.offset == i*CHUNKSIZE):
         ev_count += 1
         if (ev_count == TEST_COUNT):
            ed.shutdown()
         return
      print('FAIL: Req at offset {0}; read: {1}'.format(req.offset, req.buf))
      fail_count += 1
   
   def aio_phase2():
      f.seek(0)
      i = 0
      while (True):
         data = f.read(CHUNKSIZE)
         if (len(data) == 0):
            break
         if (len(data) < CHUNKSIZE):
            raise ValueError('Last block blockingly read contained {0} bytes: {1}.'.format(len(data), data))
         (j,) = struct.unpack('>L', data[:4])
         if (j == i):
            i += 1
            continue
         raise ValueError('Read bytes {0} from index {1}.'.format(data, i*CHUNKSIZE))
   
   def results_check():
      if (fail_count):
         raise Exception("{0} of {1} of {2} tests failed.".format(fail_count, ev_count, TEST_COUNT))
      if (ev_count != TEST_COUNT):
         raise Exception("Only got results for {0} of {1} tests (no confirmed failures).".format(ev_count, TEST_COUNT))
      print('{0} of {1} tests succeeded; no confirmed failures.'.format(ev_count, TEST_COUNT))
   
   f = open(fn,'w+b')
   
   fail_count = 0
   ev_count = 0
   sc.handle_overflow.new_listener(ofhandler)
   sc.handle_signals.new_listener(handle_signals)
   
   for i in range(TEST_COUNT):
      buf = bytearray(CHUNKSIZE)
      buf[:4] = struct.pack('>L', i)
      aio_m.io((EAIORequest(LIO_WRITE, buf, f, i*CHUNKSIZE, callback=aio_wres_process),))
   
   print('== Write test ==')
   ed.set_timer(50, ed.shutdown)
   ed.event_loop()
   results_check()
   print('== Checking written data ==')
   aio_phase2()
   print('...no errors detected.')
   print('== Read test ==')
   
   class AIORS(AIORequest):
      pass
   
   for i in range(TEST_COUNT):
      buf = bytearray(CHUNKSIZE)
      aio_m.io((EAIORequest(LIO_READ, buf, f, i*CHUNKSIZE, callback=aio_rres_process),))
   
   fail_count = 0
   ev_count = 0
   ed.event_loop()
   results_check()
   
   f.close()
   os.remove(fn)


if (__name__ == '__main__'):
   _selftest()
