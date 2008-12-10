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

# Note that the code in _signal is not thread-safe or reentrant; its functions
# should only be called from a single thread, and not from python signal
# handlers.

from . import _signal

def _selftest():
   import fcntl
   import os
   import select
   import signal
   import sys
   import time
   from signal import SIGUSR1
   print('==== Setup ====')
   (read_fd, write_fd) = os.pipe()
   fcntl.fcntl(write_fd, fcntl.F_SETFL, fcntl.fcntl(write_fd,fcntl.F_GETFL) | os.O_NONBLOCK)
   _signal.sighandler_install(SIGUSR1,_signal.SA_RESTART)
   _signal.set_wakeup_fd(write_fd)
   
   ppid = os.getpid()
   
   if (os.fork() == 0):
      time.sleep(1)
      os.close(sys.stdin.fileno())
      os.close(sys.stdout.fileno())
      os.close(sys.stderr.fileno())
      for i in range(1024):
         time.sleep(0.0001)
         os.kill(ppid,SIGUSR1)
      sys.exit()
   
   print('==== Catching signals. ====')
   timeout = time.time() + 10
   sigcount = 0
   while (timeout > time.time()):
      try:
         (r,w,e) = select.select([read_fd],[],[],0.1)
      except select.error:
         pass
      if (r):
         os.read(read_fd,1024)
         (signals, overflow) = _signal.saved_signals_get()
         print('WE GET SIGNAL: {0!a}'.format(signals))
         if (overflow):
            print ('...overflowed.')
         sigcount += len(signals)
   
   print('Caught {0} signals.'.format(sigcount))
   if (sigcount == 0):
      raise Exception("Expected more signals.")
   
