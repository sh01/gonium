#!/usr/bin/env python
#Copyright 2007, 2008 Sebastian Hagen
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

"""
This is the gonium event multiplexing module. It's a simple eventing framework
implementing the observer pattern.

Basic usage:
 - Code that wants to publish events that might be interested to more than
   one callee instantiate an EventMultiplexer and put it in a well-defined
   place. If they want to publish several types of events, they can do this
   repeatedly.
 - Code that wants to listen to such events calls the relevant
   EventMultiplexer's new_listener() method with one argument: the desired
   callee. This call will return an EventListener object.
   If the listening code keeps the EventListener object around, they can
   unregister at any time by calling its close() method.
 - Every time the EventMultiplexer instance is called, it will call all
   callbacks subscribed to it with the same positional and keyword arguments
   it was called with.
 - Exceptions thrown by callees are logged with priority 40.
"""

import logging
from collections import Callable
from copy import copy as copy_

_logger = logging.getLogger('gonium.event_multiplexing')
_log = _logger.log


class EventMultiplexer:
   """Class for passing events to set of listeners"""
   def __init__(self, parent:object):
      self.parent = parent
      self.listeners = []
   
   def new_listener(self, handler:Callable) -> 'EventListener':
      """Return new EventListener based on this multiplexer"""
      return EventListener(self, handler)
   
   def _listener_subscribe(self, listener):
      """Subscribe a new listener"""
      if (listener in self.listeners):
         raise ValueError('{0} is already tracking listener {1!a}.'.format(self, listener))
      self.listeners.append(listener)
   
   def _listener_unsubscribe(self, listener):
      """Unsubscribe a previously subscribed listener"""
      self.listeners.remove(listener)

   def __call__(self, *args, **kwargs):
      """Multiplex event"""
      for listener in copy_(self.listeners):
         listener.callback(*args, **kwargs)

   def close(self):
      """Unsubscribe all active listeners"""
      while (self.listeners != []):
         self.listeners[-1].close()


class EventListener:
   """Class for receiving events from a multiplexer"""
   def __init__(self, multiplexer:EventMultiplexer, callback:Callable):
      self._multiplexer = multiplexer
      self.callback = callback
      multiplexer._listener_subscribe(self)
      
   def __eq__(self, other) -> bool:
      """Compare for equality"""
      return (isinstance(other, EventListener) and
         (self.callback == other.callback) and 
         (self.multiplexer == other.multiplexer))
   
   def __ne__(self, other) -> bool:
      """Compare for inequality"""
      return (not (self == other))
      
   def __hash__(self, other) -> int:
      """Compute hash value of instance"""
      return hash((self.callback, self._multiplexer))
    
   def close(self):
      """Unsubscribe from multiplexer"""
      self._multiplexer._listener_unsubscribe(self)
      self._multiplexer = None


class EventAggregator(EventMultiplexer):
   """Class for aggregating multiple event multiplexers into one"""
   def __init__(self, multiplexers=(), *args, **kwargs):
      EventMultiplexer.__init__(self, *args, **kwargs)
      self.listeners_in = {}
      for m in multiplexers:
         self.multiplexer_subscribe(m)
      
   def multiplexer_register(self, m):
      """Register specified multiplexer """
      self.listeners_in[m] = m.new_listener()
      
   def multiplexer_unregister(self, m):
      """Unregister specified multiplexer"""
      listener = self.listeners_in.pop(m)
      listener.close()

   def close(self):
      """Unregister all active listeners, in both directions"""
      EventMultiplexer.close(self)
      for listener in self.listeners_in.values():
         listener.close()


class DSEventAggregator(EventAggregator):
   """Class for aggregating multiple event multiplexers while only passing every Nth event (DS: downsampling)"""
   def __init__(self, n:int, *args, **kwargs):
      EventAggregator.__init__(self, *args, **kwargs)
      self.n = n
      self.i = 0

   def __call__(self, *args, **kwargs):
      """Pass on event to listeners iff it is the Nth in series."""
      self.i = (self.i + 1) % self.n
      if (self.i == 0):
         EventAggregator.__call__(self, *args, **kwargs)

