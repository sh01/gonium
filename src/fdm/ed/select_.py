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

from heapq import heappop, heappush
import logging
import select

from ..exceptions import CloseFD
from . import _ed_register
from ._base import EventDispatcherBase

_logger = logging.getLogger('gonium.fdm.ed.select_')
_log = _logger.log

class EventDispatcherPollBase(EventDispatcherBase):
   def __init__(self, **kwargs):
      EventDispatcherBase.__init__(self, **kwargs)
      self._fdml = [None]*len(self._fdwl)
      self._poll = self.CLS_POLL()
   
   def _fdl_sizeinc(self, *args, **kwargs):
      """Increase size of fdlists to at least the specified size"""
      EventDispatcherBase._fdl_sizeinc(self,*args, **kwargs)
      self._fdml += [None]*(len(self._fdwl) - len(self._fdml))
   
   def fd_wrap(self, fd:int, *args, **kwargs):
      """Return FD wrapper based on this ED and specified fd"""
      rv = EventDispatcherBase.fd_wrap(self, fd, *args, **kwargs)
      self._fdml[fd] = 0
      return rv
   
   def _fdcb_read_r(self,fd):
      mask_old = self._fdml[fd]
      self._fdml[fd] |= self.POLLIN
      if (mask_old == 0):
         self._poll.register(fd, self._fdml[fd])
         return
      self._poll.modify(fd, self._fdml[fd])
      
   def _fdcb_read_u(self,fd):
      mask = self._fdml[fd]
      if (mask == 0):
         return
      mask &= ~self.POLLIN
      self._fdml[fd] = mask
      if (mask == 0):
         self._poll.unregister(fd)
         return
      self._poll.modify(fd, mask)
      
   def _fdcb_write_r(self,fd):
      mask_old = self._fdml[fd]
      self._fdml[fd] |= self.POLLOUT
      if (mask_old == 0):
         self._poll.register(fd, self._fdml[fd])
         return
      self._poll.modify(fd, self._fdml[fd])
      
   def _fdcb_write_u(self,fd):
      mask = self._fdml[fd]
      if (mask == 0):
         return
      mask &= ~self.POLLOUT
      self._fdml[fd] = mask
      if (mask == 0):
         self._poll.unregister(fd)
         return
      self._poll.modify(fd, mask)
   
   def event_loop(self):
      """Process events and timers until shut down."""
      timers = self._timers
      fdwl = self._fdwl
      poll = self._poll.poll
      POLLIN = self.POLLIN
      POLLOUT = self.POLLOUT
      POLLERR = self.POLLERR
      POLLHUP = self.POLLHUP
      while (not self._shutdown_pending):
         if (timers != []):
            timeout = self._timers[0].time
         else:
            timeout = -1

         # FD event processing
         try:
            events = poll(timeout)
         except IOError as exc:
            if (exc.errno == 4):
               # EINTR
               continue
            raise
         for (fd, event) in events:
            fdw = fdwl[fd]
            try:
               if (event & POLLIN):
                  fdw.process_readability()
               if (event & POLLOUT):
                  fdw.process_writability()
               if (event & POLLHUP):
                  fdw.process_hup()
               if (event & POLLERR):
                  if (fdw):
                     fdw.close()
            except CloseFD:
               fdw.close()
            except Exception as exc:
               _log(40, 'Caught exception from fd event processing code:', exc_info=True)
               if (fdw):
                  fdw.close()
         
         # Timer processing
         # Thread-safety here?
         if (timers == []):
            continue
         timers_exp = []
         while (timers[0].time < time.time()):
            timers_exp.append(heappop(timers))
         
         for timer in timers_exp:
            timer.fire()
            if (timer):
               heappush(timers,timer)


if (hasattr(select,'epoll')):
   class EventDispatcherEpoll(EventDispatcherPollBase):
      CLS_POLL = select.epoll
      POLLIN = select.EPOLLIN
      POLLPRI = select.EPOLLPRI
      POLLOUT = select.EPOLLOUT
      POLLERR = select.EPOLLERR
      POLLHUP = select.EPOLLHUP
   _ed_register(EventDispatcherEpoll)

if (hasattr(select,'poll')):
   class EventDispatcherPoll(EventDispatcherPollBase):
      CLS_POLL = select.poll
      POLLIN = select.POLLIN
      POLLPRI = select.POLLPRI
      POLLOUT = select.POLLOUT
      POLLERR = select.POLLERR
      POLLHUP = select.POLLHUP
      POLLNVAL = select.POLLNVAL
   _ed_register(EventDispatcherPoll)

