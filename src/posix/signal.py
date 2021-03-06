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

import fcntl
import logging
import os

from . import _signal
from ._signal import SigSet, SA_NOCLDSTOP, SA_ONSTACK, SA_RESETHAND, SA_RESTART, SA_NOCLDWAIT, SA_NODEFER
from ..event_multiplexing import EventMultiplexer

_logger = logging.getLogger('gonium.posix.signal')
_log = _logger.log

class SignalCatcher:
   """Signal-catching class; shouldn't be instantiated more than once.
      Note that while they aren't listed in the auto-generated docs, you can
      also access all attributes and methods of gonium.posix._signal as
      attributes of objects of this type."""
   _m = _signal
   def __init__(self, ed, bufsize:int=256):
      self._pipe_setup(ed)
      self.bufsize = bufsize
      self.sd_buffers_resize(bufsize)
      self._m.set_wakeup_fd(self._pipe_w)
   
   def _pipe_setup(self, ed):
      """Build signal pipe"""
      (pipe_r, pipe_w) = os.pipe()
      self._pipe_r = pipe_r
      self._pipe_w = pipe_w
      fcntl.fcntl(pipe_w, fcntl.F_SETFL, fcntl.fcntl(pipe_w,fcntl.F_GETFL) | os.O_NONBLOCK)
      if (ed is None):
         return
      self._pipe_r_fdw = ed.fd_wrap(pipe_r)
      self._pipe_r_fdw.process_readability = self._wakeup
      self._pipe_r_fdw.read_r()
   
   def handle_overflow(self):
      """Called on overflow. This implementation does nothing."""
      pass
   
   def handle_signals(self, siginfo:_m.SigInfo):
      """Handle signal. This implementation does nothing."""
      pass
   
   def __getattr__(self, name):
      """Forward attribute access to module"""
      return getattr(self._m,name)
   
   def _wakeup(self):
      """Fetch signals and read and discard data from read end of wrapped pipe"""
      d = os.read(self._pipe_r, 10240)
      if (not d):
         self._pipe_r_fdw.close()
      (sd, overflow) = self._m.saved_signals_get()
      if (overflow):
         self.handle_overflow()
      try:
         self.handle_signals(sd)
      except Exception:
         _log(40, 'Exception in signal handler called on siginfos'
                  '{0}:'.format(sd), exc_info=True)


class EMSignalCatcher(SignalCatcher):
   """SignalCatcher subclass that automatically sets handle_signals() and
      handle_overflow() to seperate EventMultiplexers."""
   def __init__(self, *args, **kwargs):
      SignalCatcher.__init__(self, *args, **kwargs)
      self.handle_signals = EventMultiplexer(self)
      self.handle_overflow = EventMultiplexer(self)


def _selftest():
   import fcntl
   import os
   import select
   import signal
   import sys
   import time
   from signal import SIGUSR1
   
   from ..fdm import ED_get
   from .._debugging import streamlogger_setup; streamlogger_setup()
   
   print('==== Setup ====')
   (read_fd, write_fd) = os.pipe()
   fcntl.fcntl(write_fd, fcntl.F_SETFL, fcntl.fcntl(write_fd,fcntl.F_GETFL) | os.O_NONBLOCK)
   _signal.sighandler_install(SIGUSR1,_signal.SA_RESTART)
   _signal.set_wakeup_fd(write_fd)
   
   ppid = os.getpid()
   
   sigcount_send = 1024
   if (os.fork() == 0):
      time.sleep(1)
      os.close(sys.stdin.fileno())
      os.close(sys.stdout.fileno())
      os.close(sys.stderr.fileno())
      for i in range(sigcount_send):
         time.sleep(0.0001)
         os.kill(ppid,SIGUSR1)
      sys.exit()
   
   ed = ED_get()()
   sc = SignalCatcher(ed)
   sigcount = 0
   
   def sighandler(siginfo_l):
      nonlocal sigcount
      for si in siginfo_l:
         sigcount += 1
         print('WE GET SIGNAL: {0!a}'.format(si))
   sc.handle_signals = sighandler
   def ofhandler():
      print('...overflowed.')
   sc.handle_overflow = ofhandler
   ed.set_timer(10, ed.shutdown)
   
   print('==== Catching signals. ====')
   ed.event_loop()
   
   print('Caught {0} of {1} signals.'.format(sigcount, sigcount_send))
   if (sigcount == 0):
      raise Exception("Expected more signals.")
   
if (__name__ == '__main__'):
   _selftest()
