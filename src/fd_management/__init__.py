#!/usr/bin/env python
#Copyright 2004,2005,2006,2007 Sebastian Hagen
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

import errno
import os
import sys
import socket
import select
import time
import types
import logging
import signal
import types
import thread
import re
import popen2
import signal
from sets import Set as set
import warnings

try:
   import fcntl
except ImportError:
   warnings.warn("fd_management failed to import fcntl. File locking will not be available.")
   fcntl = None

try:
   import termios
except ImportError:
   warnings.warn("fd_management failed to import termios. File locking will not be available.")
   termios = None
   

class FDManagementError(StandardError):
   pass

class FDStateError(FDManagementError):
   pass


logger = logging.getLogger('gonium.fd_management')

class FDWrap(object):
   logger = logging.getLogger('gonium.fd_management.FDWrap')
   log = logger.log
   def __init__(self, fd, parent, file=None):
      self.fd = fd
      self.parent = parent
      self.file = file
      self.active = True
      
   def fileno(self):
      return self.fd
   
   def __int__(self):
      return self.fd

   def __eq__(self, other):
      return (isinstance(other, self.__class__) and (self.fd == other.fd))
   
   def __ne__(self, other):
      return (not self.__eq__(other))

   def __cmp__(self, other):
      if (self.fd < other.fd):
         return -1
      if (self.fd > other.fd):
         return 1
      if (self.fd == other.fd):
         return 0

   def __hash__(self):
      return hash(self.fd)
   
   def fd_read(self):
      """Process readability event"""
      self.parent.fd_read(self)
      
   def fd_write(self):
      """Process writability event"""
      self.parent.fd_write(self)
   
   def fd_err(self):
      """Process error event."""
      self.parent.fd_err(self)
   
   def fd_oob(self):
      """Process OOB data."""
      self.parent.fd_err(self)
   
   def fd_hup(self):
      """Process HUP event on fd."""
      self.log(20, 'FD %r got HUP event.')
   
   def close(self):
      """Close fd, make parent forget about us, and forget about any and all associations"""
      if (not self):
         raise ValueError("%r has been closed earlier. Leave me alone." % (self,))
      self.log(20, 'Shutting down %r.' % (self,))
      self.parent.event_dispatcher.fd_unregister_all(self)
      if (self.file):
         if (isinstance(self.file, socket.socket)):
            try:
               self.file.shutdown(2)
            except socket.error:
               pass
         self.file.close()

      else:
         try:
            os.close(self.fd)
         except OSError:
            pass
      
      if (self.parent):
         try:
            self.parent.fd_forget(self)
         except Exception:
            self.log(40, 'Error fd_forget callback on closing fd %r:' % (self,), exc_info=True)

      self.active = False
   
   def __nonzero__(self):
      return bool(self.active)
   
   def __repr__(self):
      return '%s(%r, %r)' % (self.__class__.__name__, self.fd, self.parent)


class TimerTSShutdown(object):
   """Special timer delay/interval value, used for functions to be called on
      process shutdown."""
   order_min = 0
   def __init__(self, order):
      """Instantiate. Order specifies in which order a timer with this ts is
         executed relative to other ones with the same type at program
         shutdown, and must be convertible to a float. Instances with a higher
         order compare as bigger, and their timers therefore get executed later."""
      assert (self.order_min <= order <= self.order_infty)
      self.order = order
      
   def __cmp__(self, other):
      """Any instance of this class compares as bigger than anything that can be
         converted to a float, and smaller than anything that can't, except other
         instances of this class"""
      if (isinstance(other, TimerTSShutdown)):
         if (self.order < other.order):
            return -1
         elif (self.order > other.order):
            return 1
         return 0
      try:
         fv = float(other)
      except (TypeError, ValueError):
         return -1
      else:
         return 1
      
   def __repr__(self):
      return '%s(%r)' % (self.__class__.__name__, self.order)

TimerTSShutdown.order_infty = 'temp'
TimerTSShutdown.order_infty = TimerTSShutdown(0)

class DEHBuilder(object):
   """Container for convenience functions to instantiate 
      DispatchedEventHandler* classes.
      
      The basic idea is that we want the EventDispatcher* instances to offer
      a constructor for each of the DispatchedEventHandler* classes, offering
      a somewhat more convenient interface for instantiating many of those
      objects associated with the same EventDispatcher* instance.
      The builder-functions will be collected as attributes of DEHBuilder."""
   pass


class DispatchedEventHandlerMeta(type):
   """Metaclass for DispatchedEventHandler classes"""
   def __init__(cls_self, name, *cls_args, **cls_kwargs):
      """Process a newly created DispatchedEventHandler* class.
      
      This implements the DEHBuilder functionality."""
      def builder(self, *args, **kwargs):
         return cls_self(self, *args, **kwargs)
      
      builder.__name__ = name
      builder = types.UnboundMethodType(builder, None, DEHBuilder)
      setattr(DEHBuilder,name,builder)


class EventDispatcherBase(DEHBuilder):
   """General event dispatcher base class"""
   logger = logging.getLogger('gonium.fd_management.ED.Base')
   log = logger.log
   lock_timers = thread.allocate_lock()
   ts_omega = TimerTSShutdown(TimerTSShutdown.order_min)
   def __init__(self, interrupt_pipe_build=True):
      self.timers_active = []
      self.shutdown_trigger = False
      self.shutdown_executing = False

      if (interrupt_pipe_build):
         self.notify_pipe = PipeInterrupter(self)
         self.notify = self.notify_pipe.notify
      else:
         self.notify_pipe = self.notify = None
   
   def timer_register(self, timer):
      """Start tracking timer"""
      self.log(15, '%r registering timer %r.' % (self, timer))
      self.lock_timers.acquire()
      try:
         self.timers_active.append(timer)
         self.timers_active.sort()
      finally:
         self.lock_timers.release()
      
   def timer_unregister(self, timer):
      """Stop tracking timer"""
      self.log(15, '%r unregistering timer %r.' % (self, timer))
      self.lock_timers.acquire()
      try:
         self.timers_active.remove(timer)
      finally:
         self.lock_timers.release()

   def timers_finite_exist(self):
      """Return whether there are any timers guaranteed to be executed after
         finite time"""
      if not (self.timers_active):
         return False
      return (self.timers_active[0].expire_ts < self.ts_omega)

   def timers_process(self, now=None, finite_skip=False):
      """Execute all expired timers.
         If present, <now> specifies the current time; this is a hack to
         execute timers with special TS values (e.g. TimerTSShutdown)"""
      if (now is None):
         now = time.time()

      timers_active_next = []
      order_change = False
      expired_timers = []
      self.lock_timers.acquire()
      try:
         timers_active = self.timers_active
         while (timers_active and (timers_active[0].expire_ts <= now)):
            timer = timers_active.pop(0)
            if (finite_skip and (timer.expire_ts < self.ts_omega)):
               timers_active_next.append(timer)
               order_change = True
               continue
            expired_timers.append(timer)
            if (timer.persistence):
               timer.expire_ts_bump()
               timers_active_next.append(timer)
               order_change = True

         timers_active_next.extend(timers_active)
         self.timers_active = timers_active = timers_active_next
         if (order_change):
            timers_active.sort()

      finally:
         self.lock_timers.release()
      
      list_empty = []
      while (expired_timers != list_empty):
         expired_timer = expired_timers.pop(0)
         try:
            expired_timer.function_call()
         except:
            self.log(40, 'Exception in Timer %r: ' % (expired_timer,), exc_info=True)
      
      # Very special case. This doesn't actually use timers since it should
      # be possible to call shutdown_start() from a signal handler; however,
      # it is in spirit extremely similar to a timer.
      if ((not self.shutdown_executing) and (self.shutdown_trigger)):
         self.log(50, 'Executing shutdown sequence.')
         self.shutdown_executing = True
         self.shutdown()
         self.shutdown_trigger = False
         self.shutdown_executing = False

   def shutdown_start_signal(self, *args, **kwargs):
      """Signal handler for shutdown triggering"""
      self.shutdown_start()

   def shutdown_start(self):
      """Trigger shutdown of event dispatcher"""
      if (self.shutdown_executing or self.shutdown_trigger):
         # We heard you the first time.
         return
      self.shutdown_trigger = True
      if (self.notify_pipe):
         self.notify_pipe.notify()
   
   def timers_stop_byattribute(self, attr_name, attr_val):
      """Stop all timers whose attribute <attr_name> has value <attr_val>"""
      for timer in self.timers_active[:]:
         if (getattr(timer,attr_name) == attr_val):
            timer.stop()
   
   def timers_stop_byfunction(self, function):
      """Stop all timers with specified callback"""
      self.timers_stop_byattribute('function', function)
   
   def timers_stop_byparent(self, parent):
      """Stop all timers with specified parent"""
      self.timers_stop_byattribute('parent', parent)

   def timers_stop_all(self):
      """Stop all timers."""
      self.log(16, 'Unregistering all active timers.')
      self.lock_timers.acquire()
      try:
         del(self.timers_active[:])
      finally:
         self.lock_timers.release()

   def shutdown(self):
      """Shut down all connections managed by this event dispatcher and clear the timer list"""
      raise NotImplementedError()

   def fd_register_read(self, fd):
      """Add fd to read set; should be implemented by subclass"""
      raise NotImplementedError()
      
   def fd_register_write(self, fd):
      """Add fd to write set; should be implemented by subclass"""
      raise NotImplementedError()
   
   def fd_register_oob(self, fd):
      """Add fd to error set; should be implemented by subclass"""
      raise NotImplementedError()
   
   def fd_unregister_read(self, fd):
      """Remove fd from read set; should be implemented by subclass"""
      raise NotImplementedError()
      
   def fd_unregister_write(self, fd):
      """Remove fd from write set; should be implemented by subclass"""
      raise NotImplementedError()
   
   def fd_unregister_oob(self, fd):
      """Remove fd from error set; should be implemented by subclass"""
      raise NotImplementedError()

   def fd_unregister_all(self, fd):
      """Remove fd from all event sets; should be implemented by subclass"""
      raise NotImplementedError()

   def event_loop(self):
      """Run the event loop of this dispatcher; should be implemented by subclass"""
      raise NotImplementedError()


class EventDispatcherSelect(EventDispatcherBase):
   """Event dispatcher based on select.select()."""
   logger = logging.getLogger('gonium.fd_management.ED.Select')
   log = logger.log
   def __init__(self):
      self.fds_readable = []
      self.fds_writable = []
      self.fds_errable = []
      EventDispatcherBase.__init__(self)
      
   def __fd_register(self, seq, fd):
      """Remove fd from specified event sequence"""
      if (fd in seq):
         raise StandardError('%r is already contained in seq %r at %r.' % (fd, seq, self))
      seq.append(fd)
      
   def shutdown(self):
      """Shut down all connections managed by this event dispatcher and clear the timer list"""
      for fd in (self.fds_readable + self.fds_writable + self.fds_errable):
         if (fd):
            fd.close()
      try:
         self.timers_process(now=TimerTSShutdown(TimerTSShutdown.order_infty), finite_skip=True)
      finally:
         self.timers_stop_all()
      
   def fd_register_read(self, fd):
      """Add fd to read set"""
      self.__fd_register(self.fds_readable, fd)
      
   def fd_register_write(self,fd):
      """Add fd to write set"""
      self.__fd_register(self.fds_writable, fd)
      
   def fd_register_oob(self,fd):
      """Add fd to write set"""
      self.__fd_register(self.fds_errable, fd)

   def fd_unregister_read(self, fd):
      """Remove fd from read set"""
      self.fds_readable.remove(fd)
      
   def fd_unregister_write(self, fd):
      """Remove fd from write set"""
      self.fds_writable.remove(fd)

   def fd_unregister_oob(self, fd):
      """Remove fd from error set"""
      self.fds_errable.remove(fd)

   def fd_unregister_all(self, fd):
      """Remove fd from all event sets"""
      for seq in (self.fds_readable, self.fds_writable, self.fds_errable):
         if (fd in seq):
            seq.remove(fd)

   def event_loop(self):
      """Run this select event loop."""
      fds_readable = self.fds_readable
      fds_writable = self.fds_writable
      fds_errable = self.fds_errable
   
      list_empty = []
      while (1):
         if (not self.timers_finite_exist()):
            if ((list_empty == fds_readable) and
                (list_empty == fds_writable) and
                (list_empty == fds_errable)):
               self.log(45, 'No waitable objects or timers left active. Leaving select loop.')
               break
         
            timeout = None
         else:
            timeout = max((self.timers_active[0].expire_ts - time.time()), 0)
         try:
            fds_readable_now, fds_writable_now, fds_errable_now = select.select(fds_readable, fds_writable, fds_errable, timeout)
         except select.error, exc:
            if (exc.args[0] == 4):
               # Interrupted system call; can happen on signals
               continue

         for (waiting_fd_list, handler_name) in (
             (fds_readable_now, 'fd_read'),
             (fds_writable_now, 'fd_write'),
             (fds_errable_now, 'fd_err')):
            for fd in waiting_fd_list:
               if (fd.active is False):
                  continue
               event_handler = getattr(fd, handler_name)
               try:
                  event_handler()
               except Exception:
                  if (fd):
                     self.log(40, 'Failed to execute %r.%s Trying to close. Error:' % (fd, handler_name), exc_info=True)
                     try:
                        fd.close()
                     except OSError:
                        pass
                  else:
                     self.log(30, 'Failed to execute handler on already closed fd %r.%s. Error:' % (fd, handler_name), exc_info=True)

         self.timers_process()


class EventDispatcherPoll(EventDispatcherBase):
   """Event dispatcher based on select.poll()"""
   from select import POLLIN, POLLPRI, POLLOUT, POLLERR, POLLHUP, POLLNVAL
   logger = logging.getLogger('gonium.fd_management.ED.Poll')
   log = logger.log
   
   def __init__(self):
      self.fds_events = {}
      self.fdws = {}
      self.poll = select.poll()
      EventDispatcherBase.__init__(self)
   
   def fd_fa_update(self, file_alike):
      """Update file-like object associated with fd"""
      self.fdws[file_alike.fileno()] = file_alike
   
   def events_lookup(self, file_alike):
      if not (file_alike in self.fds_events):
         self.fds_events[file_alike] = 0
         self.fdws[file_alike.fileno()] = file_alike
         
      return self.fds_events[file_alike]

   def shutdown(self):
      """Shut down all connections managed by this event dispatcher and clear the timer list"""
      for fd in self.fds_events.keys():
         if (fd):
            fd.close()
      try:
         self.timers_process(now=TimerTSShutdown(TimerTSShutdown.order_infty), finite_skip=True)
      finally:
         self.timers_stop_all()

   def fd_register_read(self, fd):
      """Add fd to read set"""
      events = self.events_lookup(fd)
      events |= self.POLLIN
      self.fds_events[fd] = events
      self.poll.register(fd, events)
      
   def fd_register_write(self, fd):
      """Add fd to write set"""
      events = events = self.events_lookup(fd)
      events |= self.POLLOUT
      self.fds_events[fd] = events
      self.poll.register(fd, events)
   
   def fd_register_oob(self, fd):
      """Add fd to error set"""
      events = events = self.events_lookup(fd)
      events |= self.POLLPRI
      self.fds_events[fd] = events
      self.poll.register(fd, events)
   
   def fd_unregister_read(self, fd):
      """Remove fd from read set"""
      events = self.fds_events[fd] & ~self.POLLIN
      if (events == 0):
         del(self.fds_events[fd])
         del(self.fdws[fd.fileno()])
         self.poll.unregister(fd)
      else:
         self.fds_events[fd] = events
         self.poll.register(fd, events)
      
   def fd_unregister_write(self, fd):
      """Remove fd from write set"""
      events = self.fds_events[fd] & ~self.POLLOUT
      if (events == 0):
         del(self.fds_events[fd])
         del(self.fdws[fd.fileno()])
         self.poll.unregister(fd)
      else:
         self.fds_events[fd] = events
         self.poll.register(fd, events)
   
   def fd_unregister_oob(self, fd):
      """Remove fd from error set"""
      events = self.fds_events[fd] & ~self.POLLPRI
      if (events == 0):
         del(self.fds_events[fd])
         del(self.fdws[fd.fileno()])
         self.poll.unregister(fd)
      else:
         self.fds_events[fd] = events
         self.poll.register(fd, events)

   def fd_unregister_all(self, fd):
      """Remove fd from all event sets"""
      if (fd in self.fds_events):
         del(self.fds_events[fd])
         del(self.fdws[fd.fileno()])
         self.poll.unregister(fd)

   def event_loop(self):
      """Run this poll event loop"""
      # some aliases for performance
      dict_empty = {}
      fds_events = self.fds_events
      fdws = self.fdws
      timers_finite_exist = self.timers_finite_exist
      poll = self.poll
      POLLIN = self.POLLIN
      POLLOUT = self.POLLOUT
      POLLPRI = self.POLLPRI
      POLLERR = self.POLLERR
      POLLNVAL = self.POLLNVAL
      POLLHUP = self.POLLHUP
      
      
      while (1):
         if (not timers_finite_exist()):
            if (fds_events == dict_empty):
               self.log(45, 'No waitable objects or timers left active. Leaving poll loop.')
               break
         
            timeout = None
         else:
            timeout = max((self.timers_active[0].expire_ts - time.time()), 0)*1000
         try:
            event_data = poll.poll(timeout)
         except select.error, exc:
            if (exc.args[0] == 4):
               # Interrupted system call; can happen on signals
               continue

         for (fd, events) in event_data:
            try:
               fdw = fdws[fd]
            except KeyError:
               # Connection handlers can close other connections than their own.
               continue
            
            if (fdw.active is False):
               continue
            
            for (eventmask, event_handler) in (
               (POLLIN, fdw.fd_read),
               (POLLOUT, fdw.fd_write),
               (POLLPRI, fdw.fd_oob),
               (POLLERR, fdw.fd_err),
               (POLLNVAL, fdw.fd_err),
               (POLLHUP, fdw.fd_hup)):
               if ((eventmask & events) == 0):
                  continue
               
               try:
                  event_handler()
               except Exception:
                  if (fdw.active):
                     self.log(40, 'Failed to execute handler %s on fd %r. Trying to close. Error:' % (event_handler, fdw), exc_info=True)
                     try:
                        fdw.close()
                     except OSError:
                        pass
                  else:
                     self.log(30, 'Failed to execute handler %s on already closed fd %r. Error:' % (event_handler, fdw), exc_info=True)
               if (fdw.active is False):
                  break

         self.timers_process()
      



class DispatchedEventHandlerBase:
   """Base class for types that get events from event dispatchers"""
   __metaclass__ = DispatchedEventHandlerMeta
   def __init__(self, event_dispatcher):
      self.event_dispatcher = event_dispatcher


class BufferingBase(DispatchedEventHandlerBase):
   """Base class for event-handling types that buffer data"""
   input_handler = None
   logger = logging.getLogger('gonium.fd_management.BufferingBase')
   log = logger.log
   def __init__(self, event_dispatcher, input_handler=None, close_handler=None):
      DispatchedEventHandlerBase.__init__(self, event_dispatcher)

      self.buffers_input = {}
      self.buffers_output = {}

      self.address = None
      self.target = None
      self.closing = False
      
      if not (input_handler is None):
         self.input_handler = input_handler
      if not (close_handler is None):
         self.close_handler = close_handler

   def connection_up(self):
      """Return whether this instance has any open fds"""
      return bool(self.buffers_input)

   def fd_err(self, fd):
      """Deal with ready for input/output and error conditions on waitable objects."""

      if not ((fd in self.buffers_output) or (fd in self.buffers_input)):
         self.log(40, "This instance (%s; to %s) is not responsible for errable fd %s passed to fd_err. Trying to close it anyway." % (self, self.target, fd))
         
      fd.close()

   def fd_read(self, fd):
      """Read and buffer input"""
      data_new_read = False
      if (hasattr(fd, 'fileno')):
         fd_num = fd.fileno()
      else:
         fd_num = int(fd)
      
      if not (fd in self.buffers_input):
         raise ValueError('Fd %s is not in self.buffers_input' % (fd,))
      try:
         # select may fail with EBADF ("Bad file descriptor")
         # what about poll?
         p = select.poll()
         p.register(fd_num, select.POLLIN)
         while (len(p.poll(0)) > 0):
            #os.read may fail with OSError: [Errno 104] Connection reset by peer
            new_data = os.read(fd_num, 1048576)
            if (new_data != ''):
               self.buffers_input[fd] += new_data
               data_new_read = True
            else:
               self.log(20, 'Regular shutdown on %s noticed at fd_read().' % (self,))
               self.clean_up()
               data_new_read = False #will have been cleared by clean_up()
               break
      
      except (socket.error, OSError, select.error):
         self.log(20, 'Error reading from fd %s connected to %s. Closing connection. Error:' % (fd, self.target), exc_info=True)
         self.clean_up()
      else:
         if (data_new_read):
            self.input_process(fd)

   def input_process(self, fd):
      """Process newly buffered input"""
      self.input_handler(fd)

   def close_process(self, fd):
      """Process closing of fd"""
      self.close_handler(fd)

   def fd_write(self, fd):
      """Write as much buffered output to specified fd as possible"""
      if not (fd in self.buffers_output):
         raise ValueError('Instance %r is not currently managing fd %r.' % (self, fd))
      if (fd.active is False):
         raise FDStateError('Fd %r has been closed.' % (fd,))
      
      buffer_output = self.buffers_output[fd]
      if (buffer_output != ''):
         #Ok, we have something to send and the socket claims to be ready.
         try:
            #select may fail with EBADF ("Bad file descriptor")
            # what about poll?
            p = select.poll()
            p.register(int(fd), select.POLLOUT)
            if (len(p.poll(0)) > 0):
               #os.write could potentially fail with OSError: [Errno 104] Connection reset by peer
               bytes_sent = os.write(fd, buffer_output)
               if (bytes_sent >= len(buffer_output)):
                  #We are done here. Nothing to send left.
                  self.buffers_output[fd] = buffer_output = ''
               elif (bytes_sent > 0):
                  buffer_output = self.buffers_output[fd] = buffer_output[bytes_sent:]
               else:
                  #Connection has been closed
                  self.log(20, 'Regular shutdown on %s noticed at .fd_write().' % (self,))
                  buffer_output = ''
                  self.fd_shutdown_error_write(fd)
               
         except (socket.error, OSError, select.error):
            self.log(20, 'Error writing to socket connected to %s. Closing connection. Error:' % (self.address,), exc_info=True)
            self.buffers_output[fd] = buffer_output = ''
            self.fd_shutdown_error_write(fd)
         else:
            if ((len(buffer_output) < 1) and (fd.active)):
               #We managed to clear our output buffer. Remove this fd from the global potentially_writeable_object-list to avoid a busy loop.
               try:
                  self.event_dispatcher.fd_unregister_write(fd)
               except ValueError:
                  pass

   def fd_shutdown_error_write(self, fd):
      """Shutdown specified fd as result of failing to write data to it"""
      del(self.buffers_output[fd])
      if (fd in self.buffers_input):
         closing_last = self.closing
         self.closing = True
         self.fd_read(fd)
         self.closing = closing_last
      if (fd):
         fd.close()

   def send_data(self, data, fd=None):
      """Send data to our waitable object."""
      if (not self.buffers_output):
         self.log(30, "Unable to output %r to %s since I don't currently have any open output objects." % (data, self.target))
         return False
      elif (fd is None):
         #automatic selection; should at least work as long as there is exactly one
         fd = self.buffers_output.keys()[0]
      elif (fd not in self.buffers_output):
         self.log(30, "Unable to output %r to %s through %r since I don't have that fd open." % (data, self.target, fd))
         return False
      
      if (self.logger.isEnabledFor(10)):
         self.log(10, '< ' + repr(data))
      self.buffers_output[fd] += data

      if ((fd in self.buffers_output) and (self.buffers_output[fd])):
         self.event_dispatcher.fd_register_write(fd)
      self.fd_write(fd)

   def shutdown(self):
      """Try to flush as much output as we immediately can, then clean up."""
      for fd in self.buffers_output:
         self.fd_write(fd)

      self.clean_up()

   def fd_forget(self, fd):
      """Forget a transfer object."""
      # This is ugly, but should be enough to make sure that we do forget about
      # the fds, even if the final callbacks fail.
      closing_last = self.closing
      self.closing = True
      try:
         self.close_process(fd)
      finally:
         if (fd in self.buffers_input):
           del(self.buffers_input[fd])
         
         if (fd in self.buffers_output):
            del(self.buffers_output[fd])

      self.closing = closing_last

   def clean_up(self):
      """Shutdown all fds and discard remaining buffered output."""
      for fd in (self.buffers_output.keys() + self.buffers_input.keys()):
         if (fd):
            fd.close()

   def __repr__(self):
      return '<%s fds_reading:%s fds_writing:%s peer address:%s>' % (self.__class__.__name__, self.buffers_input.keys(), self.buffers_output.keys(), self.address)


class SockStreamBinary(BufferingBase):
   logger = logging.getLogger('gonium.fd_management.SockStreamBinary')
   log = logger.log
   
   def connection_init(self, address, address_family=socket.AF_INET):
      """Open a tcp connection to the target ip and port."""
      self.log(10, 'Connecting to %s.' % (address,))
      self.target = self.address = address
      socket_new = socket.socket(address_family, socket.SOCK_STREAM)
      socket_new.setblocking(0)
      self.socket = socket_new
      fd = FDWrap(socket_new.fileno(), self, socket_new)
      self.buffers_input[fd] = ''
      self.buffers_output[fd] = ''
      
      try:
         socket_new.connect(address)
      except socket.error, exc:
         if (exc.args[0] != 115):
            raise
         # Connect didn't complete immediately
         self.connect_finished = False
         self.event_dispatcher.fd_register_write(fd)
      else:
         self.connect_finished = True
         self.event_dispatcher.fd_register_read(fd)
   
   def fd_write(self, fd):
      """React to a writability condition on this socket"""
      if (self.connect_finished):
         BufferingBase.fd_write(self, fd)
      else:
         err = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
         if (err):
            self.log(30, 'Connection to %r failed. Error: %d(%s)' % (self.address, err, errno.errorcode[err]))
            fd.close()
         else:
            self.connect_finished = True
            self.event_dispatcher.fd_register_read(fd)
            
            if not (self.buffers_output[fd]):
               self.event_dispatcher.fd_unregister_write(fd)
            else:
               BufferingBase.fd_write(self, fd)

   def send_data(self, data, fd=None):
      """Send data to our waitable object."""
      if (self.connect_finished):
         return BufferingBase.send_data(self, data, fd)
      
      if (fd is None):
         #automatic selection; should at least work as long as there is exactly one
         fd = self.buffers_output.keys()[0]
      elif (fd not in self.buffers_output):
         self.log(30, "Unable to output %r to %s through %r since I don't have that fd open." % (data, self.target, fd))
         return False
         
      self.log(10, '< ' + repr(data))
      self.buffers_output[fd] += data

   def input_process(self, fd):
      self.input_handler()
   
   def __repr__(self):
      return '<%s to %r at %s>' % (self.__class__.__name__, self.target, id(self))


class SockStreamLinebased(SockStreamBinary):
   logger = logging.getLogger('gonium.fd_management.SockStreamLinebased')
   log = logger.log
   
   def __init__(self, event_dispatcher, line_delimiters=['\n'], *args, **kwargs):
      SockStreamBinary.__init__(self, event_dispatcher, *args, **kwargs)
      self.line_delimiters = line_delimiters
      self.buffer_lines = []

   def input_process(self, fd):
      """Process input in the value of the first element (as returned by .keys()) of the self.buffers_input-dictionary."""
      lines_split = [self.buffers_input[fd]]
      line_finished = False
      for line_delimiter in self.line_delimiters:
         lines_split_old = lines_split[:]
         lines_split = []
         for data_fragment in lines_split_old:
            lines_split.extend(data_fragment.split(line_delimiter))

      if (len(lines_split) > 0):
         #If the string ends with a line_delimiter, the last splitted element will be an empty string, 
         #otherwise it will contain the rest of the incomplete line. This code works in both cases.
         self.buffers_input[fd] = lines_split.pop(-1)
         
         self.buffer_lines.extend(lines_split)
         SockStreamBinary.input_process(self, fd)
      else:
         assert (len(self.buffers_input[fd]) == 0)


class SockServer(DispatchedEventHandlerBase):
   """Tcp server socket class. Accepts connections and instantiates their classes as needed."""
   logger = logging.getLogger('gonium.fd_management.SockServer')
   log = logger.log
   def __init__(self, event_dispatcher, bindargs, connect_handler,
         close_handler=None, connection_factory=SockStreamLinebased,
         address_family=socket.AF_INET, socket_type=socket.SOCK_STREAM,
         backlog=2):
      DispatchedEventHandlerBase.__init__(self, event_dispatcher)
      self.backlog = backlog
      self.connect_handler = connect_handler
      self.close_handler = close_handler
      self.connection_factory = connection_factory
      self.bindargs = bindargs
      self.address_family = address_family
      self.socket_type = socket_type
      self.backlog = backlog
      self.event_dispatcher = event_dispatcher
      self.socket_init()

   def socket_init(self):
      """Open the server socket, set its options and register the fd."""
      self.socket = socket.socket(self.address_family, self.socket_type)
      self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      self.socket.bind(*self.bindargs)
      self.socket.listen(self.backlog)
      self.fd = fd = FDWrap(self.socket.fileno(), self, self.socket)
      self.event_dispatcher.fd_register_read(fd)
      
   def fd_read(self, fd):
      """Accept a new connection on socket fd."""
      if not (fd == self.fd):
         raise ValueError('Not responsible for fd %s.' % (fd,))
      
      try:
         (new_socket, new_socket_address) = self.socket.accept()
      except socket.error, exc:
         self.log(35, 'Error on connection accept() at %r:', exc_info=True)
         return
      
      if (callable(self.connect_handler)):
         new_socket.setblocking(0)
         new_connection = self.connection_factory(self.event_dispatcher)
         new_fd = FDWrap(new_socket.fileno(), new_connection, new_socket)
         new_connection.buffers_input[new_fd] = ''
         new_connection.buffers_output[new_fd] = ''
         self.event_dispatcher.fd_register_read(new_fd)
         
         new_connection.target = new_connection.address = new_socket_address
         new_connection.connect_finished = True
         self.connect_handler(new_connection)

      else:
         self.log(40, 'Unable to use new connection; no handler set. Closing it.')
         new_socket.close()
      
   def fd_forget(self, fd):
      """Forget about specified fd"""
      if (fd != self.fd):
         raise ValueError("%r is not responsible for fd %r." % (self, fd))
      
      try:
         if (not (self.close_handler is None)):
            self.close_handler(fd)
      finally:
         self.fd = None
         self.socket = None

   def clean_up(self):
      if (self.fd is None):
         return
      
      self.fd.close()

      
class SockDatagram(DispatchedEventHandlerBase):
   logger = logging.getLogger('gonium.fd_management.SockDatagram')
   log = logger.log
   def __init__(self, event_dispatcher, input_handler=None, close_handler=None):
      DispatchedEventHandlerBase.__init__(self, event_dispatcher)
      self.input_handler = input_handler
      self.close_handler = close_handler
   
   def connection_init(self, sock_af=socket.AF_INET, sock_type=socket.SOCK_DGRAM, sock_protocol=0, bind_target=None):
      """Open a socket."""
      socket_new = socket.socket(sock_af, sock_type, sock_protocol)
      socket_new.setblocking(0)
      if not (bind_target is None):
         socket_new.bind(bind_target)
      fd = FDWrap(socket_new.fileno(), self, socket_new)
      self.socket = socket_new
      self.fd = fd
      self.event_dispatcher.fd_register_read(fd)

   def fd_read(self, fd):
      """Read and process data from specified fd"""
      if (not (fd == self.fd)):
         raise ValueError('%r is not responsible for fd %r.' % (self, fd))
      
      sock = fd.file
      try:
         #select may fail with EBADF ("Bad file descriptor")
         # what about poll?
         p = select.poll()
         p.register(int(fd), select.POLLIN)
         while (len(p.poll(0)) > 0):
            (data, source) = sock.recvfrom(1048576,)
            if (len(data) > 0):
               self.input_process(fd, source, data)
            else:
               self.close(fd)
               self.clean_up()
               break
            if (self.fd is None):
               # This socket may have been closed in input handler
               break
      
      except socket.error:
         self.log(30, 'Error reading from fd %s. Closing connection. Error:' % (fd, ), exc_info=True)
         self.clean_up()

   def input_process(self, fd, source, data):
      """Process input from socket"""
      self.input_handler(source, data)
      
   def fd_write(self, fd):
      """Process writability condition on socket; should be implemented by subclass if at all"""
      raise NotImplementedError
   
   def send_data(self, data, target, fd=None, flags=0):
      """Send data to our waitable object."""
      if (not self.fd):
         raise StandardError("%r unable to output %r(to %r) since I don't currently have any open output objects." % (self, data, target))
      if (fd is None):
         fd = self.fd
      if (not (fd == self.fd)):
         raise ValueError('%r is not responsible for fd %r.' % (self, fd))
      
      self.log(10, '%r< %r' % (target, data))
      return fd.file.sendto(data, flags, target)

   def fd_forget(self, fd):
      """Forget about specified fd"""
      if (fd != self.fd):
         raise ValueError("%r is not responsible for fd %r." % (self, fd))
      
      try:
         if (not (self.close_handler is None)):
            self.close_handler(fd)
      finally:
         self.fd = None
         self.socket = None

   def clean_up(self):
      if (self.fd is None):
         return
      
      self.fd.close()


class FileBinary(BufferingBase):
   """Asynchronous interface to pseudo-files"""
   logger = logging.getLogger('gonium.fd_management.FileBinary')
   log = logger.log
   def __init__(self, event_dispatcher, filename, input_handler, close_handler, mode=0777, flags=os.O_RDWR, *args, **kwargs):
      BufferingBase.__init__(self, event_dispatcher, *args, **kwargs)
      self.input_handler = input_handler
      self.close_handler = close_handler
      self.target = self.filename = filename
      self.flags = flags = flags | os.O_NONBLOCK | os.O_NOCTTY
      self.fd = FDWrap(os.open(filename, flags, mode), self)
      self.buffers_input[self.fd] = ''
      self.buffers_output[self.fd] = ''
      self.event_dispatcher.fd_register_read(fd)
      self.locked = False

   def input_process(self, fd):
      self.input_handler(self, fd)
         
   def lockf(self, operation=None, *args, **kwargs):
      """Lock opened file"""
      if not (fcntl):
         raise RuntimeError("fcntl module not present; perhaps the OS doesn't support it?")
      if not (operation):
         operation = fcntl.LOCK_EX

      return_value = fcntl.lockf(self.fd, operation, *args, **kwargs)
      self.locked = True
      self.log(20, 'Locked fd %s on file %s.' % (self.fd, self.filename))
      return return_value
      
   def fd_forget(self, fd):
      """Forget managed fd"""
      if (self.locked and (fd == self.fd)):
         try:
            fcntl.lockf(fd, fcntl.LOCK_UN)
            self.locked = False
         except:
            self.log(40, 'Failed to unlock fd %s on file %s. Error:' % (self.fd, self.filename), exc_info=True)
      
      BufferingBase.fd_forget(self,fd)
      self.fd = None

   def clean_up(self):
      self.fd.close()


class FileBinarySerialport(FileBinary):
   logger = logging.getLogger('gonium.fd_management.FileBinarySerialport')
   log = logger.log
   
   def __init__(self, event_dispatcher, filename='/dev/ttyS0', *args, **kwargs):
      if not (termios):
         raise RuntimeError("termios module not present; perhaps the os doesn't support it?")

      FileBinary.__init__(self, event_dispatcher, filename, *args, **kwargs)
      
   def tcgetattr(self):
      if (self.fd):
         return termios.tcgetattr(self.fd)
         
   def tcsetattr(self, when, attributes):
      if (self.fd):
         return termios.tcsetattr(self.fd, when, attributes)
         
   def tcsendbreak(self, duration):
      if (self.fd):
         return termios.tcsendbreak(self.fd, duration)
         
   def tcdrain(fd):
      if (self.fd):
         return termios.tcdrain(self.fd)
         
   def tcflush(self, queue):
      if (self.fd):
         return termios.tcflush(self.fd, queue)
         
   def tcflow(self, action):
      if (self.fd):
         return termios.tcflow(self.fd, action)
      
   def fd_forget(self, fd):
      """Forget managed fd"""
      if (fd):
         try:
            termios.tcflush(int(fd), termios.TCOFLUSH)
         except termios.error:
            pass
         FileBinary.fd_forget(self, fd)

#pipes to spawned processes
CHILD_REACT_NOT = 0
CHILD_REACT_WAIT = 1
CHILD_REACT_KILL = 2

class ChildRunnerBase(BufferingBase):
   """Base class for piped child-spawning."""
   logger = logging.getLogger('gonium.fd_management.ChildRunnerBase')
   log = logger.log
   def __init__(self, event_dispatcher, command, termination_handler, finish=1, *args, **kwargs):
      BufferingBase.__init__(self, event_dispatcher, *args, **kwargs)
      
      self.command = command
      self.termination_handler = termination_handler
      self.finish = finish

      self.stdin_fd = None
      self.stdout_fd = None
      self.stderr_fd = None
      self.child = None

      self.stdout_str = None
      self.stderr_str = None
      self.child_spawn()
      
   def child_spawn(self):
      """Start child process; should be implemented by subclasses"""
      raise NotImplementedError('child_spawn should be implemented by child classes.')
      
   def input_process(self, fd):
      """Process input from child process"""
      self.input_handler(self, fd)
         
   def child_kill(self, sig=signal.SIGTERM):
      """Send a signal (SIGTERM by default) to our child process."""
      if (self.child):
         self.log(30, 'Sending signal %s to process spawned by executing "%s".' % (sig, self.command,))
         os.kill(self.child.pid, sig)

   def child_poll(self):
      """Check if our child is dead, and if so poll() on it, wait() on it and if termination_handler is callable call it with the results."""
      if (self.child):
         return_code = self.child.poll()
         if (return_code == -1):
            #Process is still alive
            return False
         else:
            exit_status = self.child.wait()
            self.log(20, 'Process %d spawned by executing "%s" has finished with return code %s and exit status %s.' % (self.child.pid, self.command, return_code, exit_status))
            if (callable(self.termination_handler)):
               self.termination_handler(self, return_code, exit_status)
            
            self.clean_up()

   def fd_forget(self, fd):
      """Forget one of the fds managed by us"""
      if (fd == self.stdin_fd):
         self.stdin_fd = None
      if (fd == self.stdout_fd):
         self.stdout_fd = None
         self.stdout_str = self.buffers_input[fd]
      if (fd == self.stderr_fd):
         self.stderr_fd = None
         self.stderr_str = self.buffers_input[fd]
      
      BufferingBase.fd_forget(self, fd)
      if (len(self.buffers_input) == 0 == len(self.buffers_output)):
         #We're out of open input fds.
         self.child_poll()
         if (self.child):
            #And our child is still alive,...
            if (self.finish == CHILD_REACT_WAIT):
               #...so we WAIT for it.
               self.logging.log(30, 'Child process %d (resulted from executing "%s") has closed all connections to us, but is still active. Spawning thread to wait for it.' % (self.child.pid, self.command))
               self.thread = threading.Thread(group=None, target=self.wait_child_childthread, name=None, args=(), kwargs={})
               self.thread.setDaemon(True)
               self.thread.start()
            elif (self.finish == CHILD_REACT_KILL):
               #...so we kill it.
               sig = signal.SIGKILL
               self.child_kill(sig)
               self.child_poll()
               if (self.child):
                  self.log(45, 'Child process %d (resulted from executing "%s") has survived signal %d sent by us. Giving up.' % (self.child.pid, self.command, sig))
   
   def wait_child_childthread(self):
      """Wait() on our child, then poll() it, set a timer to report the results and finally trigger pipe_notify."""
      exit_status = self.child.wait()
      return_code = self.child.poll()
      Timer(interval=-1000, function=self.termination_handler, args=(self, return_code, exit_status), callback_kwargs={}, parent=None)
      pipe_notify.notify()

   def stream_record(self, name, fd, in_stream=False):
      """Prepare internal data structures for a new stream and register its fd."""
      self.__dict__[name] = fd
      if (in_stream):
         self.buffers_input[fd] = ''
         self.event_dispatcher.fd_register_read(fd)
      else:
         self.buffers_output[fd] = ''
      
   def clean_up(self):
      BufferingBase.clean_up(self)
      self.child = None

class ChildRunnerPopen3(ChildRunnerBase):
   '''Popen3 objects provide stdin, stdout, and stderr of the child process.'''
   logger = logging.getLogger('gonium.fd_management.ChildRunnerPopen3')
   log = logger.log
   
   def __init__(self, event_dispatcher, capturestderr=False, *args, **kwargs):
      self.capturestderr = capturestderr
      ChildRunnerBase.__init__(self, event_dispatcher, *args, **kwargs)
      
   def child_spawn(self):
      """Start child process"""
      self.child = child = popen2.Popen3(self.command, self.capturestderr)
      self.stream_record('stdin_fd', FDWrap(child.tochild.fileno(), self), in_stream=False)
      self.stream_record('stdout_fd', FDWrap(child.fromchild.fileno(), self), in_stream=True)
      
      if (child.childerr):
         self.stream_record('stderr_fd', FDWrap(child.childerr.fileno(), self), in_stream=True)

class ChildRunnerPopen4(ChildRunnerBase):
   '''Popen4 objects provide stdin to the child process, and a combined stdout+stderr stream from it.'''
   logger = logging.getLogger('gonium.fd_management.ChildRunnerPopen4')
   log = logger.log
   def child_spawn(self):
      """Start child process"""
      self.child = child = popen2.Popen4(self.command)
      fd_out = FDWrap(child.fromchild.fileno(), self)
      
      self.stream_record('stdin_fd', FDWrap(child.tochild.fileno(), self), in_stream=False)
      self.stream_record('stdout_fd', fd_out, in_stream=True)
      self.stderr_fd = fd_out


class PipeInterrupter(DispatchedEventHandlerBase):
   """Manages a pipe used to safely interrupt blocking select calls of other threads."""
   logger = logging.getLogger('gonium.fd_management.PipeInterrupter')
   log = logger.log
   def __init__(self, event_dispatcher):
      DispatchedEventHandlerBase.__init__(self, event_dispatcher)
      self.read_fd = self.write_fd = None
      self.pipe_init()

   def pipe_init(self):
      """Open and initialize the pipe,"""
      (read_fd, self.write_fd) = [FDWrap(fd, self) for fd in os.pipe()]
      fcntl.fcntl(read_fd, fcntl.F_SETFL, os.O_NONBLOCK)
      self.event_dispatcher.fd_register_read(read_fd)
      self.read_fd = read_fd
      
   def fd_read(self, fd):
      """Read data and throw it away. Reestablish pipe if it has collapsed."""
      if not (fd == self.read_fd):
         raise ValueError('Not responsible for fd %s' % (fd,))
      
      p = select.poll()
      p.register(int(fd), select.POLLIN)
      while (len(p.poll(0)) > 0):
         if (len(os.read(int(fd), 1048576)) <= 0):
            #looks like for some reason this pipe has collapsed.
            self.log(40, 'The pipe has collapsed. Reinitializing.')
            self.clean_up()
            self.pipe_init()

   def notify(self):
      """Write a byte into the input end of the pipe if pipe intact and not already readable"""
      if (self.read_fd and self.write_fd):
         os.write(int(self.write_fd), '\000')

   def fd_forget(self, fd):
      """Forget specified fd"""
      if (self.read_fd == fd):
         self.read_fd = None
      elif (self.write_fd == fd):
         self.write_fd = None
      
      if (self.read_fd):
         self.read_fd.close()
      if (self.write_fd):
         self.write_fd.close()
      
   def clean_up(self):
      self.fd_forget(None)


class Timer(DispatchedEventHandlerBase):
   '''Timer class; instantiate to start. This is one of the few thread-safe parts of fd_management,
      because it's also used to transfer information in general back to the main thread.'''
   logger = logging.getLogger('gonium.fd_management.Timer')
   log = logger.log
   def __init__(self, event_dispatcher, interval, function, parent=None, args=(), kwargs={}, persistence=False, align=False, ts_relative=True, log_level_adjust=0):
      self.event_dispatcher = event_dispatcher
      self.interval = interval
      self.function = function
      self.parent = parent
      self.args = args
      self.kwargs = kwargs
      self.persistence = bool(persistence)
      self.align = align
      
      now = time.time()
      if (ts_relative):
         self.expire_ts = now + float(interval)
         if (align):
            self.expire_ts -= (now % interval)
      else:
         self.expire_ts = interval

      self.register()

   def __cmp__(self, other):
      if (self.expire_ts < other.expire_ts):
         return -1
      if (self.expire_ts > other.expire_ts):
         return 1
      if (id(self) < id(other)):
         return -1
      if (id(self) > id(other)):
         return 1
      return 0

   def register(self):
      """Start tracking timer"""
      self.event_dispatcher.timer_register(self)
      
   def unregister(self):
      """Stop tracking timer"""
      self.event_dispatcher.timer_unregister(self)

   def function_call(self):
      """Call registered function with saved args and kwargs"""
      self.log(20, 'Executing timer %r.' % (self,))
      self.function(*self.args, **self.kwargs)
      
   def expire_ts_bump(self):
      if (not self.persistence):
         raise ValueError("%r isn't persistent." % self)
      
      now = time.time()
      
      self.expire_ts = now + self.interval
      if (self.align):
         self.expire_ts -= now % self.interval

   def __getinitargs__(self):
      return (self.interval, self.function, self.parent, self.args, self.kwargs, self.persistence, self.align)

   def __repr__(self):
      return '%s%r' % (self.__class__.__name__, self.__getinitargs__())
   
   def __str__(self):
      return '%s%s' % (self.__class__.__name__, self.__getinitargs__())

   def stop(self):
      """Stop this timer."""
      self.log(20, 'Stopping timer %r.' % (self,))
      self.unregister()


TIMERS_CALLBACK_HANDLER = 2
TIMERS_PARENT = 1

