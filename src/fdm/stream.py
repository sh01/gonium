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
import subprocess
import sys
from collections import deque
from errno import EAGAIN, ECONNRESET, EPIPE, EINPROGRESS, EINTR
from select import poll, POLLOUT
from socket import socket as socket_cls, AF_INET, SOCK_STREAM, SOL_SOCKET, \
   SO_ERROR, error as sockerr

from .exceptions import CloseFD

_logger = logging.getLogger('gonium.fd_management')
_log = _logger.log


class AsyncDataStream:
   """Class for asynchronously accessing streams of bytes of any kind.
   
   public class methods:
      build_sock_connect(ed, address, connect_callback, *, family, proto, **):
        Build instance wrapping new outgoing SOCK_STREAM connection.
   
   public instance methods:
     send_data(lines, flush): Append data lines to output buffer; if flush
       evaluates to True, also try to send it now.
     send_bytes(lines, flush): As above, but without trying to encode strings
     discard_inbuf_data(n): Discard first n bytes of buffered input
     close(): Close wrapped filelike, if open
     
   Public attributes (intended for reading only):
      fl: wrapped filelike
   Public attributes (r/w):
      size_need: amount of input to buffer before calling self.process_input()
      output_encoding: argument to pass to .encode() for encoding str
         instances passed to send_data(). Data from byte sequences-objects
         is always written unmodified.
      process_input(): process newly buffered input
      process_close(): process FD closing
   """
   def __init__(self, ed, filelike, *, inbufsize_start:int=1024,
                inbufsize_max:int=0, size_need:int=0, read_r:bool=True):
      self.fl = filelike
      for name in ('recv_into', 'readinto'):
         try:
            self._in = getattr(filelike, name)
         except AttributeError:
            continue
         break
      else:
         raise ValueError("Unable to find recv_into/readinto method on object {0!a}".format(filelike,))
      for name in ('send', 'write'):
         try:
            self._out = getattr(filelike, name)
         except AttributeError:
            continue
         break
      else:
         raise ValueError("Unable to find send/write method on object {0!a}".format(filelike,))
      
      self._fw = ed.fd_wrap(self.fl.fileno())
      self._fw.process_readability = self._process_input0
      self._fw.process_writability = self._output_write
      self._fw.process_close = self._process_close
      if (read_r):
         self._fw.read_r()
      
      self.size_need = size_need # how many bytes to read before calling input_process2()
      assert(inbufsize_start > 0)
      self._inbuf = bytearray(inbufsize_start)
      self._outbuf = deque()
      self._inbuf_size = inbufsize_start
      self._inbuf_size_max = inbufsize_max
      self._index_in = 0        # part of buffer filled with current data
      self.output_encoding = None
   
   @classmethod
   def build_sock_connect(cls, ed, address, connect_callback=None, *,
      family:int=AF_INET, type_:int=SOCK_STREAM, proto:int=0, **kwargs):
      """Nonblockingly open outgoing SOCK_STREAM/SOCK_SEQPACKET connection."""
      sock = socket_cls(family, type_, proto)
      sock.setblocking(0)
      try:
         sock.connect(address)
      except sockerr as exc:
         if (exc.errno == EINPROGRESS):
            pass
         else:
            raise
      self = cls(ed, sock, read_r=False, **kwargs)
      def connect_process():
         err = sock.getsockopt(SOL_SOCKET, SO_ERROR)
         if (err):
            _log(30, ('Async stream connection to {0} failed.'
               'Error: {1}({2})').format(address, err, errno.errorcode[err]))
            self._fw.write_u()
            raise CloseFD()
         
         self._fw.read_r()
         # Write output, if we have any pending; else, turn writability
         # notification off
         self._fw.process_writability = self._output_write
         self._output_write(_known_writable=False)
         if not (connect_callback is None):
            connect_callback(self)
      self._fw.process_writability = connect_process
      self._fw.write_r()
      return self
   
   def send_data(self, buffers:collections.Sequence, *args, **kwargs):
      """Like send_bytes(), but encodes any strings with self.output_encoding."""
      enc = self.output_encoding
      def encode(buf):
         if (hasattr(buf, 'encode')):
            buf = buf.encode(enc)
         return buf
      
      self.send_bytes(map(encode, buffers), *args, **kwargs)
      
   def send_bytes(self, buffers:collections.Sequence, flush=True):
      """Append set of buffers to pending output and attempt to push.
         Buffers elements must be bytes, bytearray, memoryview or similar."""
      assert not (isinstance(buffers, (bytes, bytearray)))
      had_pending = bool(self._outbuf)
      self._outbuf.extend(buffers)
      if (flush):
         self._output_write(had_pending, _known_writable=False)

   def discard_inbuf_data(self, bytes:int=None):
      """Discard <bytes> of in-buffered data.
      
      If bytes is unspecified or None, discard all of it."""
      if ((bytes is None) or (bytes == self._index_in)):
         self._index_in = 0
         return
      if (bytes > self._index_in):
         raise ValueError('Asked to discard {0} bytes, but only have {1} in buffer.'.format(bytes, self._index_in))
      self._inbuf[:self._index_in-bytes] = self._inbuf[bytes:self._index_in]
      self._index_in -= bytes
         
   def close(self):
      """Close wrapped fd, if currently open"""
      if (self._fw):
         self._fw.close()

   def process_close(self):
      """Process FD closing; intended to be overwritten by instance user"""
      pass

   def __bool__(self) -> bool:
      """Returns True iff our wrapped FD is still open"""
      return bool(self._fw)

   def _inbuf_resize(self, new_size:(int, type(None))=None):
      """Increase size of self.inbuf without discarding data"""
      if (self._inbuf_size >= self._inbuf_size_max > 0):
         _log(30, 'Closing {0} because buffer limit {0._inbuf_size_max} has been hit.'.format(self))
         self.close()
      if (new_size is None):
         new_size = self._inbuf_size * 2
      if (self._inbuf_size_max > 0):
         new_size = min(new_size, self._inbuf_size_max)
      self._inbuf_size = new_size
      inbuf_new = bytearray(new_size)
      inbuf_new[:self._index_in] = self._inbuf[:self._index_in]
      self._inbuf = inbuf_new

   def _process_close(self):
      """Internal method for processing FD closing"""
      try:
         self.process_close()
      finally:
         try:
            self.fl.close()
         except Exception:
            pass
         self._in = None
         self._out = None
         self._outbuf = None

   def _output_write(self, _writeregistered:bool=True,
      _known_writable:bool=True):
      """Write output and manage writability notification (un)registering"""
      firstbuf = True
      while (True):
         try:
            buf = self._outbuf.popleft()
         except IndexError:
            break
         
         try:
            rv = self._out(buf)
         except sockerr as exc:
            if ((exc.errno == EINTR) or (exc.errno == ENOBUFS)):
               break
            elif ((exc.errno == ECONNRESET) or (exc.errno == EPIPE)):
               raise CloseFD()
         
         if (firstbuf):
            firstbuf = False
            if ((0 == rv) and (_known_writable)):
               self._outbuf.appendleft(buf)
               raise CloseFD()
         
         if (rv < len(buf)):
            # This might happen if send is interrupted by a signal, but that
            # should be rare, and is probably not worth optimizing for.
            # For performance: don't copy data; instead create a memoryview
            # skipping written data
            self._outbuf.appendleft(memoryview(buf)[rv:])
            break
      
      if (bool(self._outbuf) != _writeregistered):
         if (_writeregistered):
            self._fw.write_u()
         else:
            self._fw.write_r()
   
   def _read_data(self):
      """Read and buffer input from wrapped file-like object"""
      try:
         br = self._in(memoryview(self._inbuf)[self._index_in:])
      except IOError as exc:
         if (exc.errno == EINTR):
            return
         if (exc.errno == ECONNREFUSED):
            raise CloseFD()
         raise
      if (br == 0):
         raise CloseFD()
      self._index_in += br
   
   def _process_input0(self):
      """Input processing stage 0: read and buffer bytes"""
      self._read_data()
      if (self._index_in >= self.size_need):
         self._process_input1()
      if (self._index_in >= self._inbuf_size):
         self._inbuf_resize()

   def _process_input1(self):
      """Override in subclass to insert more handlers"""
      self.process_input(self, memoryview(self._inbuf)[:self._index_in])


class AsyncLineStream(AsyncDataStream):
   """Class for asynchronously accessing line-based bytestreams"""
   def __init__(self, ed, filelike, lineseps:collections.Set=(b'\n',), **kwargs):
      AsyncDataStream.__init__(self, ed, filelike, **kwargs)
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
         self.discard_inbuf_data(line_start)
   
   def _process_input2(self, *args, **kwargs):
      self.process_input(self, *args, **kwargs)


class AsyncPopen(subprocess.Popen):
   """Popen subclass that automatically creates stdio Async*Stream instances
      for stdio streams of subprocess"""
   def __init__(self, ed, *args, stream_cls=AsyncDataStream, **kwargs):
      subprocess.Popen.__init__(self, *args, **kwargs)
      for name in ('stdin', 'stdout', 'stderr'):
         stream = getattr(self,name)
         if (stream is None):
            continue
         setattr(self, name + '_async', stream_cls(ed, stream))


class AsyncSockServer:
   """Asynchronous listening SOCK_STREAM/SOCK_SEQPACKET sockets.
   
   self.connect_process(sock, addressinfo) should be overridden by the instance
     user; it will be called once for each accepted connection.
   """
   def __init__(self, ed, address, *, family:int=AF_INET, proto:int=0,
         type_:int=SOCK_STREAM, backlog:int=16):
      self.sock = socket_cls(family, type_, proto)
      self.sock.setblocking(0)
      self.sock.bind(address)
      self.sock.listen(backlog)
      self._fw = ed.fd_wrap(self.sock.fileno())
      self._fw.process_readability = self._connect_process
      self._fw.read_r()
   
   def connect_process(self, sock:socket_cls, addressinfo):
      """Should be overridden by instance user: process new incoming connection"""
      raise NotImplementedError()
   
   def _connect_process(self):
      """Internal method: process new incoming connection"""
      while (True):
         try:
            (sock, addressinfo) = self.sock.accept()
         except sockerr as exc:
            if (exc.errno == EAGAIN):
               return
            raise
         self.connect_process(sock, addressinfo)


def _selftest(out=None):
   import os
   from . import ED_get
   from subprocess import PIPE
   from .._debugging import streamlogger_setup; streamlogger_setup()
   if (out is None):
      out = sys.stdout
   
   class D1:
      def __init__(self,l=8):
         self.i = 0
         self.l = l
      def __call__(self, stream, data, *args,**kwargs):
         self.i += 1
         
         ads_out.send_data(('line: {0!a} {1} {2}\n'.format(data.tobytes(), args, kwargs),))
         if (self.i > self.l):
            stream.close()
   
   def close_handler():
      d = 2
      ads_out.send_data(('Pipe closed, will shutdown in {0} seconds\n'.format(d),))
      ed.set_timer(d, ed.shutdown)
      
   ed = ED_get()()
   out.write('Using ED {0}\n'.format(ed))
   out.write('==== AsyncLineStream test ====\n')
   sp = AsyncPopen(ed, ('ping', '127.0.0.1', '-c', '64'), stdout=PIPE, stream_cls=AsyncLineStream)
   sp.stdout_async.process_input = D1()
   sp.stdout_async.process_close = close_handler
   # socket testing code; commented out since it needs a suitably chatty remote
   #sock = AsyncLineStream.build_sock_connect(ed, (('192.168.0.10',6667)))
   #sock.process_input = D1()
   #sock.send_data((b'test\nfoo\nbar\n',),flush=False)
   ads_out = AsyncDataStream(ed, open(out.fileno(),'wb', buffering=0, closefd=False), read_r=False)
   ads_out.output_encoding = 'ascii'
   
   ed.event_loop()
   sp.kill()

if (__name__ == '__main__'):
   _selftest(sys.stdout)
