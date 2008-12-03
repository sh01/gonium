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

# FDM Stream handling classes.

import collections
import logging

_logger = logging.getLogger('gonium.fd_management')
_log = _logger.log


class AsyncDataStream:
   """Class for asynchronously accessing streams of bytes of any kind."""
   def __init__(self, ed, filelike, inbufsize_start:int=1024,
                inbufsize_max:int=0, size_need:int=0):
      self._f = filelike
      self._fw = ed.fd_wrap(self._f.fileno())
      self._fw.process_readability = self._process_input0
      self._fw.read_r()
      
      self.size_need = size_need # how many bytes to read before calling input_process2()
      assert(inbufsize_start > 0)
      self._inbuf = bytearray(inbufsize_start)
      self._inbuf_size = inbufsize_start
      self._inbuf_size_max = inbufsize_max
      self._index_in = 0        # part of buffer filled with current data
   
   def _inbuf_resize(self, new_size=None):
      """Increase size of self.inbuf without discarding data"""
      if (self._inbuf_size >= self._inbuf_size_max >= 0):
         _log(30, 'Closing {0} because buffer limit {0._inbuf_size_max} has been hit.'.format(self))
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
         self._process_input1()
      if (self._index_in >= self._inbuf_size):
         self._inbuf_resize()

   def _process_input1(self):
      """Override in subclass to insert more handlers"""
      self.process_input(memoryview(self._inbuf)[:self._index_in])

   def inbuf_data_discard(self, bytes:int=None):
      """Discard <bytes> of in-buffered data."""
      if ((bytes is None) or (bytes == self._index_in)):
         self._index_in = 0
         return
      if (bytes > self._index_in):
         raise ValueError('Asked to discard {0} bytes, but only have {1} in buffer.'.format(bytes, self._index_in))
      self._inbuf[:self._index_in-bytes] = self._inbuf[bytes:self._index_in]
      self._index_in -= bytes


class AsyncLineStream(AsyncDataStream):
   """Class for asynchronously accessing line-based bytestreams"""
   def __init__(self, ed, filelike, lineseps:collections.Set=(b'\n',), **kwargs):
      super().__init__(ed, filelike, **kwargs)
      self._inbuf_index_l = 0
      self._ls = lineseps
      self._ls_maxlen = max([len(s) for s in lineseps])
      
   def _process_input1(self):
      """Input processing stage 1: split data into lines"""
      # Make sure we don't skip over seperators partially read earlier
      index_l = min(0, self._inbuf_index_l-self._ls_maxlen+1)
      line_start = 0
      while (True):
         line_end = None
         for sep in self._ls:
            i = self._inbuf.find(sep, index_l, self._index_in)
            if (i == -1):
               continue
            if (line_end is None):
               line_end = i+len(sep)
               continue
            line_end = min(fi,i+len(sep))
         
         if (line_end is None):
            break
         self._process_input2(memoryview(self._inbuf)[line_start:line_end])
         line_start = index_l = line_end
      if (line_start):
         self.inbuf_data_discard(line_start)
   
   def _process_input2(self, *args, **kwargs):
      self.process_input(*args, **kwargs)


def _selftest(out=None):
   import sys
   from .ed import ed_get
   from subprocess import Popen, PIPE
   if (out is None):
      out = sys.stdout
   
   class D1:
      def __init__(self,l=8):
         self.i = 0
         self.l = l
      def __call__(self, data, *args,**kwargs):
         self.i += 1
         if (self.i > self.l):
            als1.close()
         out.write('{0!a} {1} {2}\n'.format(data.tobytes(), args, kwargs))
   
   ed = ed_get()()
   out.write('Using ED {0}\n'.format(ed))
   out.write('==== AsyncLineStream test ====\n')
   sp = Popen(('ping', '127.0.0.1'), stdout=PIPE, stderr=PIPE)
   als1 = AsyncLineStream(ed, sp.stdout)
   als1.process_input = D1()
   
   ed.event_loop()

if (__name__ == '__main__'):
   _selftest(sys.stdout)
