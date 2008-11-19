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

"""Gonium event multiplexing. This is a basic event framework implementing a 
generalization of the observer pattern."""


class EventListenerRegister:
   """Baseclass for classes which register event listeners"""
   pass

class EventListenerMeta(type):
   """Metaclass for EventListeners. Implements EventListenerRegister functionality."""
   def __init__(self, name, *args, **kwargs):
      type.__init__(self, name, *args, **kwargs)
      
      def builder(elr_self, *args, **kwargs):
         return self(elr_self, *args, **kwargs)
      
      builder.name = name
      setattr(EventListenerRegister, name, builder)

class EventMultiplexer(EventListenerRegister):
   """Class for passing events to set of listeners"""
   def __init__(self, parent):
      self.parent = parent
      self.listeners = []
   
   def listener_register(self, listener):
      """Register a new listener"""
      if (listener in self.listeners):
         raise ValueError('%r is already tracking listener %r.' % (self, listener))
      self.listeners.append(listener)
   
   def listener_unregister(self, listener):
      """Unregister a previously registered listener"""
      self.listeners.remove(listener)

   def __call__(self, *args, **kwargs):
      """Multiplex event"""
      for listener in self.listeners[:]:
         listener.handler_run(*args, **kwargs)

   def clean_up(self):
      """Unregister all active listeners"""
      while (self.listeners != []):
         self.listeners[0].clean_up()


class EventListener:
   __metaclass__ = EventListenerMeta
   """Class for receiving events from a multiplexer"""
   def __init__(self, multiplexer, handler):
      self.multiplexer = multiplexer
      self.handler = handler
      multiplexer.listener_register(self)
      
   def __eq__(self, other):
      """Compare for equality"""
      return (isinstance(other, EventListener) and
         (self.handler == other.handler) and 
         (self.multiplexer == other.multiplexer))
   
   def __neq__(self, other):
      """Compare for inequality"""
      return (not (self == other))
      
   def __hash__(self, other):
      """Compute hash value of instance"""
      return hash((self.handler, self.multiplexer))
      
   def handler_run(self, *args, **kwargs):
      """Run event handler"""
      self.handler(self, *args, **kwargs)
    
   def clean_up(self):
      """Unregister from multiplexer"""
      self.multiplexer.listener_unregister(self)
      self.multiplexer = None


class CCBEventListener(EventListener):
   """Event listener with separate callback at unregistering (CCB: cleanup call back)"""
   def __init__(self, multiplexer, close_handler, *args, **kwargs):
      EventListener.__init__(self, multiplexer, *args, **kwargs)
      self.close_handler = close_handler
   
   def clean_up(self):
      """Unregister from multiplexer"""
      try:
         self.close_handler(self)
      except:
         EventListener.clean_up(self)
         raise
      else:
         EventListener.clean_up(self)


class EventAggregator(EventMultiplexer):
   """Class for aggregating multiple event multiplexers into one"""
   def __init__(self, multiplexers=None, *args, **kwargs):
      EventMultiplexer.__init__(self, *args, **kwargs)
      self.listeners_in = {}
      if not (multiplexers is None):
         for m in multiplexers:
            self.multiplexer_register(m)
      
   def listener_in_close_handle(self, listener):
      """Handle closing of listener for incoming events"""
      del(self.listeners_in[listener.multiplexer])
      
   def multiplexer_register(self, m):
      """Register specified multiplexer """
      self.listeners_in[m] = CCBEventListener(m, self.listener_in_close_handle, self)
      
   def multiplexer_unregister(self, m):
      """Unregister specified multiplexer"""
      listener = self.listeners_in.pop(m)
      listener.clean_up()

   def clean_up(self):
      """Unregister all active listeners, in both directions"""
      EventMultiplexer.clean_up(self)
      for listener in self.listeners_in.values():
         listener.clean_up()


class DSEventAggregator(EventAggregator):
   """Class for aggregating multiple event multiplexers while only passing every Nth event (DS: downsampling)"""
   def __init__(self, n, *args, **kwargs):
      EventAggregator.__init__(self, *args, **kwargs)
      self.n = n
      self.i = 0

   def __call__(self, *args, **kwargs):
      """Pass on event to listeners iff it is the Nth in series."""
      self.i = (self.i + 1) % self.n
      if (self.i == 0):
         EventAggregator.__call__(self, *args, **kwargs)

