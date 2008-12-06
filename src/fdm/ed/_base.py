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

import logging
import fcntl
import os
from types import MethodType
from heapq import heappush

_logger = logging.getLogger('gonium.src.fdm')
_log = logger.log

class EventDispatcherBase:
   FDC_INITIAL = 16
   def __init__(self, fdc_initial:int=0):
      fdc_initial = fdc_initial or self.FDC_INITIAL
      self._fdwl = [None]*fdc_initial
      self._shutdown_pending = False
      self._timers = []
   
   def fd_wrap(self, fd:int, set_nonblock:bool=True):
      """Return FD wrapper based on this ED and specified fd"""
      i = int(fd)
      if (i >= len(self._fdwl)):
         self._fdl_sizeinc(i+1)
      if not (self._fdwl[i] is None):
         return self._fdwl[i]
      
      rv = FDWrap(self, fd)
      if (set_nonblock):
         fcntl.fcntl(i, fcntl.F_SETFL, fcntl.fcntl(i,fcntl.F_GETFL) | os.O_NONBLOCK)
      
      self._fdwl[i] = rv
      return rv
   
   def add_timer(self, timer):
      """Add timer for delayed processing."""
      # Thread-safety here?
      heappush(self._timers,timer)
      
   def event_loop(self):
      """Run event loop; should be implemented in subclass."""
      raise NotImplementedError()
   
   def shutdown(self):
      """Shutdown event loop."""
      self._shutdown_pending = True
   
   def _fdl_sizeinc(self, newsize:int):
      """Increase size of fdlists to at least the specified size"""
      l = len(self._fdwl)
      if (l >= newsize):
         return
      
      newsize = 2**int(math.ceil(math.log(newsize,2)))
      self._fdwl += [None]*(newsize-l)
   
   def _fdcb_read_r(self,fd):
      raise NotImplementedError()
   def _fdcb_read_u(self,fd):
      raise NotImplementedError()
   def _fdcb_write_r(self,fd):
      raise NotImplementedError()
   def _fdcb_write_u(self,fd):
      raise NotImplementedError()


class FDWrap:
   __slots__ = ('_ed', 'fd', 'process_readability', 'process_writability',
      'process_hup', 'process_close')
   """FD associated monitored by a specific ED. Events are returned by calling
      attributes:
      process_readability() for READ
      process_writability() for WRITE
      process_close() for connection close
      process_hup() for hup events
   """
   def __init__(self, ed:EventDispatcherBase, fd:int):
      self._ed = ed
      self.fd = fd
      self.process_readability = None
      self.process_writability = None
      self.process_hup = self.close
      
   # For documentation only
   def read_r(self):
      """Register fd for read events"""
      raise NotImplementedError("Should be overridden by __init__")
   def read_u(self):
      """Unregister fd for read events"""
      raise NotImplementedError("Should be overridden by __init__")
   def write_r(self):
      """Register fd for write events"""
      raise NotImplementedError("Should be overridden by __init__")
   def write_u(self):
      """Unregister fd for write events"""
      raise NotImplementedError("Should be overridden by __init__")

   def read_r(self):
      """Register fd for reading."""
      self._ed._fdcb_read_r(self.fd)
   def read_u(self):
      """Unegister fd for reading."""
      self._ed._fdcb_read_u(self.fd)
   def write_r(self):
      """Register fd for writng."""
      self._ed._fdcb_write_r(self.fd)
   def write_u(self):
      """Unregister fd for writng."""
      self._ed._fdcb_write_u(self.fd)

   def close(self):
      """Close this fd."""
      if (self._ed._fdwl[self.fd] is self):
         self._ed._fdwl[self.fd] = None
      try:
         self.process_close()
      except Exception:
         _log(40, 'Error in fd-close handler:', exc_info=True)
      self.read_u()
      self.write_u()
      os.close(self.fd)
      self._ed = None
   def process_close(self):
      """Process FD closing; this implementation does nothing"""
      pass
   
   def fileno(self):
      """Return wrapped fd."""
      return self.fd
   def __int__(self):
      """Return wrapped fd."""
      return self.fd
   
   def __hash__(self):
      return hash(self.fd)
   def __bool__(self):
      return not (self._ed is None)
   def __eq__(self, other):
      return (self.fd == other.fd)
   def __ne__(self,other):
      return (self.fd != other.fd)
