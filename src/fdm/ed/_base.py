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
import math
import numbers
import os
import threading
from collections import Callable
from errno import EBADF
from heapq import heappush, heapify
from time import time as time_
from types import MethodType

from ...event_multiplexing import EventMultiplexer

_logger = logging.getLogger('gonium.src.fdm')
_log = _logger.log


class _Timer:
   """Asynchronous timer, to be fired by an FDM event dispatcher."""
   def __init__(self, ed, interval:numbers.Real, callback:Callable,
         args=(), kwargs={}, *, parent=None, persist=False, align=False,
         interval_relative=True):
      self._ed = ed
      self._interval = interval
      self._callback = callback
      self._cbargs = args
      self._cbkwargs = kwargs
      self.parent = parent
      self._persist = persist
      self._align = align
      expire_ts = interval
      now = time_()
      if (interval_relative):
         expire_ts += now
      if (align):
         expire_ts -= (expire_ts % interval)
      
      self._expire_ts = expire_ts
      if (self._ed is None):
         return
      self._ed._register_timer(self)
   
   def cancel(self):
      """Stop timer, cancelling sheduled callback."""
      self._ed._unregister_timer(self)
      self._expire_ts = None

   def fire(self):
      """Fire timer, executing callback and (if persistent) bumping expire time"""
      try:
         self._callback(*self._cbargs, **self._cbkwargs)
      finally:
         if (self._persist):
            if (self._align):
               self._expire_ts = time_() + self._interval
               self._expire_ts -= (self._expire_ts % self._interval)
            else:
               self._expire_ts += self._interval - ((time_() - self._expire_ts) % self._interval)
         else:
            self._expire_ts = None

   # comparison functions
   # __eq__, __ne__ and __hash__ are by default based on id(); this works just
   # fine for this class.
   def __lt__(self, other):
      return ((self._expire_ts < other._expire_ts) or
         ((self._expire_ts == other._expire_ts) and (id(self) < id(other))))
   def __gt__(self, other):
      return ((self._expire_ts > other._expire_ts) or
         ((self._expire_ts == other._expire_ts) and (id(self) > id(other))))
   def __le__(self, other):
      return not (self > other)
   def __ge__(self,other):
      return not (self < other)
   
   def __bool__(self):
      """Return whether timer is active (i.e. still pending for firing)"""
      return not (self._expire_ts is None)


class EventDispatcherBase:
   """Base class for event dispatchers."""
   FDC_INITIAL = 16
   def __init__(self, fdc_initial:int=0):
      fdc_initial = fdc_initial or self.FDC_INITIAL
      self._fdwl = [None]*fdc_initial
      self.em_shutdown = EventMultiplexer(self)
      self._shutdown_pending = False
      self._timers = []
   
   def fd_wrap(self, fd:int, set_nonblock:bool=True, fl=None):
      """Return FD wrapper based on this ED and specified fd
      
      fl, if specified, specifies a filelike to call .close() on instead of
      calling os.close() on fd when the fdw is closed. Specifying it is
      strongly recommended if one exists; many python fd wrapper objects
      will insist on closing the wrapped fd at deallocation at the latest.
      If it had already been closed before then, it might have been reused
      in the meantime, leading to hard-to-trace EBADF bugs."""
      i = int(fd)
      if (i >= len(self._fdwl)):
         self._fdl_sizeinc(i+1)
      if not (self._fdwl[i] is None):
         return self._fdwl[i]
      
      rv = _FDWrap(self, fd, fl=fl)
      if (set_nonblock):
         fcntl.fcntl(i, fcntl.F_SETFL, fcntl.fcntl(i,fcntl.F_GETFL) | os.O_NONBLOCK)
      
      self._fdwl[i] = rv
      return rv
   
   def set_timer(self, *args, **kwargs) -> _Timer:
      """timer(*args, **kwargs) -> _Timer
         
         Set a new timer registered with this event dispatcher. Arguments are
         passed unchanged to _Timer().
         """
      return _Timer(self, *args, **kwargs)
   
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


class EventDispatcherBaseTT(EventDispatcherBase):
   """Base class for event dispatchers with threadsafe timer adding and
      removing."""
   def __init__(self, *args, **kwargs):
      EventDispatcherBase.__init__(self, *args, **kwargs)
      self._timer_lock = threading.Lock()
   
   def _register_timer(self, timer):
      """Threadsafely register timer for delayed execution handling."""
      self._timer_lock.acquire()
      try:
         heappush(self._timers, timer)
      finally:
         self._timer_lock.release()
   
   def _unregister_timer(self, timer):
      """Threadsafely unregister timer."""
      self._timer_lock.acquire()
      try:
         self._timers.remove(timer)
         heapify(self._timers)
      finally:
         self._timer_lock.release()
      timer._expire_ts = None


def _donothing(*args, **kwargs):
   """Does nothing."""
   pass


class _FDWrap:
   __slots__ = ('fd', 'process_readability', 'process_writability',
      'process_hup', 'process_close', '_ed', '_fl')
   """FD associated monitored by a specific ED. Events are returned by calling
      attributes:
      process_readability() for READ
      process_writability() for WRITE
      process_close() for connection close
      process_hup() for hup events
   """
   def __init__(self, ed:EventDispatcherBase, fd:int, fl=None):
      self._ed = ed
      self.fd = fd
      self._fl = fl
      self.process_readability = None
      self.process_writability = None
      self.process_close = _donothing
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
      self.read_u()
      self.write_u()
      if (self._ed._fdwl[self.fd] is self):
         self._ed._fdwl[self.fd] = None
      
      if not (self._fl is None):
         self._fl.close()
      else:
         os.close(self.fd)
      
      try:
         self.process_close()
      except Exception:
         _log(40, 'Error in fd-close handler:', exc_info=True)

      self._fl = False
      self._ed = None
   
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

