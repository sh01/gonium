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

import os
import signal
from collections import deque

from ._aio import LIO_READ, LIO_WRITE, AIOManager, AIORequest

def _selftest():
   import struct
   from ..fdm import ED_get
   from .signal import SignalCatcher, SA_RESTART
   from .._debugging import streamlogger_setup; streamlogger_setup()
   
   aio_m = AIOManager()
   ed = ED_get()()
   sc = SignalCatcher(ed)
   TEST_COUNT = 1024
   
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
   
   def aio_wres_process():
      if (not pending_events):
         return
      rv = aio_m.get_results(pending_events)
      pending_events.clear()
      for req in rv:
         if (req.rc > 0):
            continue
         print('req {0} failed with rc {1}.'.format(req, req.rc))
   
   def aio_rres_process():
      fail_count = 0
      if (not pending_events):
         return
      rv = aio_m.get_results(pending_events)
      pending_events.clear()
      
      for req in rv:
         (i,) = struct.unpack('>L', req.buf)
         if (req.offset == i*4):
            print('SUCC: Req at offset {0}'.format(req.offset))
            continue
         print('FAIL: Req at offset {0}; read: {1}'.format(req.offset, req.buf))
         fail_count += 1
      return (len(rv), fail_count)
   
   def aio_phase2():
      f.seek(0)
      i = 0
      while (True):
         data = f.read(4)
         if (len(data) == 0):
            break
         if (len(data) < 4):
            raise ValueError('Last block blockingly read contained {0} bytes: {1}.'.format(len(data), data))
         (j,) = struct.unpack('>L', data)
         if (j == i):
            i += 1
            continue
         raise ValueError('Read bytes {0} from index {1}.'.format(data, i*4))
         
   
   f = open(fn,'w+b')
   
   ed.set_timer(5, ed.shutdown)
   sc.handle_overflow = ofhandler
   sc.handle_signals = handle_signals
   
   for i in range(TEST_COUNT):
      buf = struct.pack('>L', i)
      aio_m.io(AIORequest(LIO_WRITE, buf, f, i*4))
   
   print('== Write test ==')
   ed.event_loop()
   aio_wres_process()
   aio_phase2()
   print('...passed.')
   print('== Read test ==')
   
   class AIOSR(AIORequest):
      pass
   
   for i in range(TEST_COUNT):
      buf = bytearray(4)
      req = AIOSR(LIO_READ, buf, f, i*4)
      req.buf = buf
      aio_m.io(req)
   
   ed.set_timer(5, ed.shutdown)
   ed.event_loop()
   (ev_count, failure_count) = aio_rres_process()
   if (failure_count):
      raise Exception("{0} of {1} of {2} read tests failed.".format(failure_count, ev_count, TEST_COUNT))
   print('{0} of {1} read tests succeeded; no confirmed failures.'.format(ev_count, TEST_COUNT))
   f.close()
   #os.remove(fn)


if (__name__ == '__main__'):
   _selftest()
