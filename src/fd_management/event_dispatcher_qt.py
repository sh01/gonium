#!/usr/bin/env python
#Copyright 2007 Sebastian Hagen
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
import time
import math
import types

from PyQt4.QtCore import QObject, QSocketNotifier, QTimer, SIGNAL, QThread

from gonium.fd_management import EventDispatcherBase

# Stupidly, these appear to be staticmethods. Might as well make them global
# then.
connect = QObject.connect
disconnect = QObject.disconnect


class EventDispatcherQTFD:
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
      connect(self.qsn_oob, SIGNAL('activated(int)'), self.fd_oob)
   
   def fd_read(self, fd):
      self.fdw.fd_read()
   def fd_write(self, fd):
      self.fdw.fd_write()
   def fd_oob(self, fd):
      self.fdw.fd_oob()
   
   def register_read(self):
      self.qsn_read.setEnabled(True)

   def register_write(self):
      self.qsn_write.setEnabled(True)

   def register_oob(self):
      self.qsn_oob.setEnabled(True)

   def unregister_read(self):
      self.qsn_read.setEnabled(False)
      return not (False is self.qsn_read.isEnabled() is self.qsn_write.isEnabled() is self.qsn_oob.isEnabled())

   def unregister_write(self):
      return not (False is self.qsn_read.isEnabled() is self.qsn_write.isEnabled() is self.qsn_oob.isEnabled())

   def unregister_oob(self):
      return not (False is self.qsn_read.isEnabled() is self.qsn_write.isEnabled() is self.qsn_oob.isEnabled())

   def unregister_all(self):
      for qsn in (self.qsn_read, self.qsn_write, self.qsn_oob):
         if (qsn.isEnabled()):
            qsn.setEnabled(False)
   
   def clean_up(self):
      self.unregister_all()
      disconnect(self.qsn_read, SIGNAL('activated(int)'), self.fd_read)
      disconnect(self.qsn_write, SIGNAL('activated(int)'), self.fd_write)
      disconnect(self.qsn_oob, SIGNAL('activated(int)'), self.fd_oob)
      self.qsn_read.deleteLater()
      self.qsn_write.deleteLater()
      self.qsn_oob.deleteLater()
      self.qsn_read = None
      self.qsn_write = None
      self.qsn_oob = None


class EventDispatcherQT(EventDispatcherBase):
   """Event dispatcher class based on QT event loop"""
   def __init__(self, *args, **kwargs):
      self.fds = {}
      self.tp_qttimer = None
      
      EventDispatcherBase.__init__(self, *args, **kwargs)
   
   def shutdown(self):
      """Shut down all connections managed by this event dispatcher and clear the timer list"""
      raise NotImplementedError()

   def fd_register(self, fdw, eventtype):
      """Start monitoring specified eventtype on fd"""
      fd = int(fdw)
      if (fd in self.fds):
         edqf = self.fds[fd]
      else:
         edqf = EventDispatcherQTFD(fdw)
         self.fds[fd] = edqf
      
      getattr(edqf, ('register_' + eventtype))()

   def fd_unregister(self, fdw, eventtype):
      """Stop monitoring specified eventtype on fd"""
      fd = int(fdw)
      edqf = self.fds[fd]
      if (not getattr(edqf, ('unregister_' + eventtype))()):
         # No more events registered; forget about fd
         edqf.clean_up()
         del(self.fds[fd])

   def fd_register_read(self, fdw):
      """Add fd to read set"""
      self.fd_register(fdw, 'read')
      
   def fd_register_write(self, fdw):
      """Add fd to write set"""
      self.fd_register(fdw, 'write')
   
   def fd_register_oob(self, fdw):
      """Add fd to error set"""
      self.fd_register(fdw, 'oob')
   
   def fd_unregister_read(self, fdw):
      """Remove fd from read set"""
      self.fd_unregister(fdw, 'read')
      
   def fd_unregister_write(self, fdw):
      """Remove fd from write set"""
      self.fd_unregister(fdw, 'write')
   
   def fd_unregister_oob(self, fdw):
      """Remove fd from error set"""
      self.fd_unregister(fdw, 'oob')

   def fd_unregister_all(self, fdw):
      """Remove fd from all event sets"""
      self.fds.pop(int(fdw)).clean_up()

   def timer_processing_prepare(self):
      """Set QTimer to call timers_process() in time for next time expiration"""
      if (self.timers_active):
         timer = self.tp_qttimer = QTimer()
         connect(timer, SIGNAL('timeout()'), self.timers_process)
         self.tp_qttimer.start(max(math.ceil((self.timers_active[0].expire_ts - time.time())*1000),0))
      else:
         self.tp_qttimer = None

   def timer_register(self, *args, **kwargs):
      """Register a new timer."""
      EventDispatcherBase.timer_register(self, *args, **kwargs)
      if not (self.tp_qttimer is False):
         if not (self.tp_qttimer is None):
            self.tp_qttimer.stop()
         self.timer_processing_prepare()

   def timers_process(self, *args, **kwargs):
      """Process expired timers."""
      self.tp_qttimer = False
      EventDispatcherBase.timers_process(self, *args, **kwargs)
      self.timer_processing_prepare()

   def event_loop(self, qtapp):
      """Run the event loop of this dispatcher"""
      if (self.tp_qttimer is None):
         self.timer_processing_prepare()
      
      qtapp.exec_()

