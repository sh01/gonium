#!/usr/bin/env python
#Copyright 2007,2008 Sebastian Hagen
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

# For lack of easy access to Python3.0->QT4 bindings, this code hasn't been
# tested  since being ported to python3.0 gonium. It is likely to be broken.

import os
import time
import math
import types

from PyQt4.QtCore import QObject, QSocketNotifier, QTimer, SIGNAL, QThread

from ._base import EventDispatcherBaseTT

# Stupidly, these appear to be staticmethods. Might as well make them global
# then.
connect = QObject.connect
disconnect = QObject.disconnect


class _EventDispatcherQTFD:
   """Internal fdw wrapper used by EventDispatcherQT"""
   def __init__(self, fdw):
      self.fdw = fdw
      self.qsn_read = QSocketNotifier(int(self.fdw), QSocketNotifier.Read)
      self.qsn_write = QSocketNotifier(int(self.fdw), QSocketNotifier.Read)
      self.qsn_oob = QSocketNotifier(int(self.fdw), QSocketNotifier.Exception)
      self.qsn_read.setEnabled(False)
      self.qsn_write.setEnabled(False)
      self.qsn_oob.setEnabled(False)
      connect(self.qsn_read, SIGNAL('activated(int)'), self.fd_read)
      connect(self.qsn_write, SIGNAL('activated(int)'), self.fd_write)
   
   def process_readability(self, fd):
      self.fdw.process_readability()
   
   def process_writability(self, fd):
      self.fdw.process_writability()
   
   def read_r(self):
      self.qsn_read.setEnabled(True)

   def write_r(self):
      self.qsn_write.setEnabled(True)

   def read_u(self):
      self.qsn_read.setEnabled(False)

   def write_u(self):
      self.qsn_write.setEnabled(False)

   def _unregister_all(self):
      for qsn in (self.qsn_read, self.qsn_write, self.qsn_oob):
         if (qsn.isEnabled()):
            qsn.setEnabled(False)
   
   def close(self):
      self._unregister_all()
      disconnect(self.qsn_read, SIGNAL('activated(int)'), self.fd_read)
      disconnect(self.qsn_write, SIGNAL('activated(int)'), self.fd_write)
      disconnect(self.qsn_oob, SIGNAL('activated(int)'), self.fd_oob)
      self.qsn_read.deleteLater()
      self.qsn_write.deleteLater()
      self.qsn_oob.deleteLater()
      self.qsn_read = None
      self.qsn_write = None
      self.qsn_oob = None


class EventDispatcherQT(EventDispatcherBaseTT):
   """Event dispatcher class based on QT event loop"""
   def __init__(self, *args, **kwargs):
      self.fds = {}
      self.tp_qttimer = None
      
      EventDispatcherBaseTT.__init__(self, *args, **kwargs)
   
   def shutdown(self):
      """Shut down all connections managed by this event dispatcher and clear the timer list"""
      raise NotImplementedError()

   def _fd_register(self, fdw, eventtype):
      """Start monitoring specified eventtype on fd"""
      fd = int(fdw)
      if (fd in self.fds):
         edqf = self.fds[fd]
      else:
         edqf = _EventDispatcherQTFD(fdw)
         self.fds[fd] = edqf
      
      getattr(edqf, ('register_' + eventtype))()

   def _fd_unregister(self, fdw, eventtype):
      """Stop monitoring specified eventtype on fd"""
      fd = int(fdw)
      edqf = self.fds[fd]
      if (not getattr(edqf, ('unregister_' + eventtype))()):
         # No more events registered; forget about fd
         edqf.clean_up()
         del(self.fds[fd])

   def _fdcb_read_r(self, fdw):
      """Add fd to read set"""
      self.fd_register(fdw, 'read')
      
   def _fdcb_write_r(self, fdw):
      """Add fd to write set"""
      self.fd_register(fdw, 'write')
   
   def _fdcb_read_u(self, fdw):
      """Remove fd from read set"""
      self.fd_unregister(fdw, 'read')
      
   def _fdcb_write_u(self, fdw):
      """Remove fd from write set"""
      self.fd_unregister(fdw, 'write')

   def _fd_unregister_all(self, fdw):
      """Remove fd from all event sets"""
      self.fds.pop(int(fdw)).close()

   def _timer_processing_prepare(self):
      """Set QTimer to call timers_process() in time for next time expiration"""
      with self.timer_lock:
         if (self._timers_active):
            timer = self.tp_qttimer = QTimer()
            connect(timer, SIGNAL('timeout()'), self._timers_process)
            self.tp_qttimer.start(max(math.ceil((self._timers[0].expire_ts - time.time())*1000),0))
         else:
            self.tp_qttimer = None

   def _register_timer(self, *args, **kwargs):
      """Register a new timer."""
      EventDispatcherBaseTT._register_timer(self, *args, **kwargs)
      if not (self.tp_qttimer is False):
         if not (self.tp_qttimer is None):
            self.tp_qttimer.stop()
         self._timer_processing_prepare()

   def _timers_process(self, *args, **kwargs):
      """Process expired timers."""
      self.tp_qttimer = False
      timers = self._timers
      # Timer processing
      with self._timer_lock:
         if (timers == []):
            return
         timers_exp = deque()
         now = ttime()
         try:
            while (timers[0]._expire_ts <= now):
               timers_exp.append(heappop(timers))
         except IndexError:
            pass
      
      while (timers_exp):
         timer = timers_exp.popleft()
         try:
            timer.fire()
         except Exception as exc:
            _log(40, 'Caught exception in timer {0}:'.format(timer), exc_info=True)
         if (timer):
            heappush(timers,timer)
      
      self._timer_processing_prepare()

   def event_loop(self, qtapp):
      """Run the event loop of this dispatcher"""
      if (self.tp_qttimer is None):
         self._timer_processing_prepare()
      
      qtapp.exec_()

