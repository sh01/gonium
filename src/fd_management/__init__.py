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

# module history:
# An early predecessor to this module appeared in an ancient gonium version
# (before the project was called that) under the name 'socket_management'.
#
# This module was obsoleted by 'fd_management' in a later gonium version for
# python 2.x.
# Python 3.x brought bytes and bytearrays, which necessitated significant
# changes to fd_management's interface, and suggested more. Since backwards
# compatibility had to be broken a second time in any case, it was decided to
# use this opportunity to completely redesign fd_management's inteface for
# increased clarity and usability.
# As such, this is the third major FD-managing module in gonium's history.
# It's currently in an early stage of implementation, and not yet usable.


import collections
import logging
import os
import socket
import sys
import time


class FDMError(Exception):
   pass

_logger = logging.getLogger('gonium.fd_management')
_log = _logger.log

class AsyncDataStream:
   """Class for asynchronously accessing streams of bytes of any kind."""
   def __init__(self, ed, inbufsize_start: int=1024, inbufsize_max:int=0, size_need:int=0):
      self._f
      self.size_need = size_need # how many bytes to read before calling input_process2()
      assert(inbufsize_start > 0)
      self._inbuf = bytearray(inbufsize_start)
      self._inbuf_size = inbufsize_start
      self._inbuf_size_max = inbufsize_max
      self._index_in = 0
   
   def _inbuf_resize(self, new_size=None):
      """Increase size of self.inbuf without discarding data"""
      if (self._inbuf_size >= self._inbuf_size_max >= 0):
         _log(30, 'Closing {0} because buffer limit {0.inbuf_size_max} has been hit.'.format(self))
         self.close()
      if (new_size is None):
         new_size = self._inbuf_size * 2
      new_size = min(new_size, self._inbuf_size_max)
      self._inbuf_size = new_size
      inbuf_new = bytearray(new_size)
      inbuf_new[:self._index_in] = self._inbuf[:self._index_in]
      self._inbuf = inbuf_new
   
   def _data_read(self):
      """Read and buffer input from wrapped file-like object"""
      br = self._f.readinto(memoryview(self._inbuf)[self._index_in:])
      if (br == 0):
         self.close()
      self._index_in += br
   
   def _process_input0(self):
      """Input processing stage 0: read and buffer bytes"""
      self._data_read()
      if (self._index_in >= self.size_need):
         self._process_input1(self)
      if (self._index_in >= self._inbuf_size):
         self._inbuf_resize()

   def _process_input1(self):
      """Override in subclass to insert more handlers"""
      self.process_input()

   def inbuf_data_discard(self, bytes:int):
      """Discard <bytes> of in-buffered data."""
      inbuf_new = bytearray(self._inbuf_size)
      inbuf_new[bytes:self._inbuf_size] = self._inbuf[bytes:self._inbuf_size]
      self._inbuf = inbuf_new


def AsyncLineStream(AsyncDataStream):
   """Class for asynchronously accessing line-based bytestreams"""
   def __init__(self, ed, lineseps:collections.Set=('n',), **kwargs):
      super().__init__(ed, **kwargs)
      self._inbuf_index_l = 0
      self._ls = lineseps
      self._ls_maxlen = max([len(s) for s in lineseps])
      
   def process_input1(self):
      """Input processing stage 1: split data into lines"""
      # Make sure we don't skip over seperators partially read earlier
      index_l = min(0, self._inbuf_index_l-self._ls_maxlen+1)
      line_start = 0
      while (True):
         line_end = None
         for sep in self._ls:
            i = self._inbuf(sep, index_l, self._inbuf_size)
            if (i == -1):
               continue
            if (line_end is None):
               line_end = i+len(sep)
               continue
            line_end = min(fi,i+len(sep))
         
         if (line_end is None):
            break
         self._process_input2(self,memoryview(self._inbuf)[line_start:line_end])
         line_start = index_l = line_end
      if (line_start):
         self.inbuf_data_discard(line_start)
   
   def process_input2(self, *args, **kwargs):
      self.process_input(*args, **kwargs)

