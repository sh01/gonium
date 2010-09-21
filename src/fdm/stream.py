#!/usr/bin/env python
#Copyright 2008, 2009 Sebastian Hagen
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
import errno
import logging
import os
import subprocess
import socket
import sys
from collections import deque
from errno import EAGAIN, ECONNRESET, EPIPE, EINPROGRESS, EINTR, ENOBUFS, \
   ECONNREFUSED, EHOSTUNREACH, ECONNRESET, ENOMEM, ECONNABORTED, ECONNRESET, \
   ETIMEDOUT
from select import poll, POLLOUT
from socket import socket as socket_cls, AF_INET, SOCK_STREAM, SOL_SOCKET, \
   SO_ERROR, error as sockerr

from ..ip_address import IPAddressBase
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
      process_input(data): process newly buffered input
      process_close(): process FD closing
   """
   _SOCK_ERRNO_TRANS = {0, EINTR, ENOBUFS, ENOMEM, EAGAIN}
   _SOCK_ERRNO_FATAL = {ECONNREFUSED, ECONNRESET, EHOSTUNREACH, ECONNABORTED,
      EPIPE, ETIMEDOUT}
   
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
      
      self._ed = ed
      self._fw = ed.fd_wrap(self.fl.fileno(), fl=self.fl)
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
      self.connected = True
      self.ssl_handshake_pending = None
      self.ssl_callback = None
   
   @classmethod
   def build_sock_connect(cls, ed, address, connect_callback=None, *,
      family:int=AF_INET, type_:int=SOCK_STREAM, proto:int=0, bind_target=None,
      **kwargs):
      """Nonblockingly open outgoing SOCK_STREAM/SOCK_SEQPACKET connection."""
      sock = socket_cls(family, type_, proto)
      sock.setblocking(0)
      s_address = address
      if (isinstance(address[0], IPAddressBase)):
         s_address = (str(address[0]), address[1])
      
      if not (bind_target is None):
         sock.bind(bind_target)
      
      try:
         sock.connect(s_address)
      except sockerr as exc:
         if (exc.errno == EINPROGRESS):
            pass
         else:
            raise
      self = cls(ed, sock, read_r=False, **kwargs)
      self.connected = False
      
      def connect_process():
         err = sock.getsockopt(SOL_SOCKET, SO_ERROR)
         if (err):
            _log(30, ('Async stream connection to {0} failed.'
               'Error: {1}({2})').format(address, err, errno.errorcode[err]))
            self._fw.write_u()
            raise CloseFD()
         
         self.connected = True
         if (self.ssl_handshake_pending):
            (ssl_args, ssl_kwargs) = self.ssl_handshake_pending
            self._do_ssl_handshake(*ssl_args, **ssl_kwargs)
         else:
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
         try:
            self._output_write(had_pending, _known_writable=False)
         except CloseFD:
            # Can't let this propagate upwards through this callpath; might
            # not have been called by an event-readiness handler for this fd.
            # Make sure we'll be called through that callpath ASAP and can
            # safely close it then instead.
            self._fw.write_r()

   def _bfs_process(self, dtr):
      """Process possibly partial block send."""
      if (self._outbuf is None):
         return
      
      self._unblock_output()
      if (dtr.get_missing_byte_count() != 0):
         self._outbuf.appendleft(dtr)
      
      if (self._outbuf):
         self._fw.write_r()

   def send_bytes_from_file(self, dtd, file, off, length):
      """Get data from specified file-like using specified dtd, and send it.
         """
      
      had_pending = bool(self._outbuf)
      if (self.ssl_callback):
         # Can't use direct fd2fd copy here, since we need to push the data
         # through the crypto stack before sending.
         mv = bytearray(length)
         def cb(dtr):
            if (self._outbuf is None):
               return
            self._unblock_output()
            if (dtr.get_missing_byte_count() == 0):
               self._outbuf.appendleft(mv)
            else:
               self._outbuf.appendleft(dtr)
            self._fw.write_r()
         dtr = dtd.new_req_fd2mem(file, mv, cb, length, off)
      else:
         dtr = dtd.new_req(file, self.fl, self._bfs_process, length, off, None)
      
      dtr.errno = EAGAIN
      
      self._outbuf.append(dtr)
      del(dtr)
      if not (self._outbuf[0] is None):
         self._fw.write_r()

   def discard_inbuf_data(self, count:int=None):
      """Discard <count> bytes of in-buffered data.
      
      If count is unspecified or None, discard all of it."""
      if ((count is None) or (count == self._index_in)):
         self._index_in = 0
         return
      if (count > self._index_in):
         raise ValueError('Asked to discard {0} bytes, but only have {1} in buffer.'.format(count, self._index_in))
      self._inbuf[:self._index_in-count] = self._inbuf[count:self._index_in]
      self._index_in -= count
      
   def close(self):
      """Close wrapped fd, if currently open"""
      if (self._fw):
         self._fw.close_by_gc()
         self._out = None
         self._in = None

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
         raise CloseFD()
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
      self._fw = None
      self._in = None
      self._out = None
      self._outbuf = None
      self.fl = None
      self.process_close()

   def _block_output(self):
      self._outbuf.appendleft(None)
   
   def _unblock_output(self):
      self._outbuf.popleft()

   def _output_write(self, _writeregistered:bool=True,
      _known_writable:bool=True):
      """Write output and manage writability notification (un)registering"""
      if (self._out is None):
         return
      
      while (True):
         try:
            buf = self._outbuf.popleft()
         except IndexError:
            break
         
         if (buf is None):
            # We're supposed to wait for some external event before sending
            # any more data, so let's do that.
            self._block_output()
            if (_writeregistered):
               self._fw.write_u()
            return
         
         if (hasattr(buf, 'queue')):
            # It's actually a DTR object with an unfinished transfer.
            if (buf.errno in self._SOCK_ERRNO_TRANS):
               buf.queue()
               self._block_output()
               continue
            if (buf.errno in self._SOCK_ERRNO_FATAL):
               raise CloseFD()
            buf.get_errors()
         
         try:
            rv = self._out(buf)
         except sockerr as exc:
            self._outbuf.appendleft(buf)
            if (exc.errno in self._SOCK_ERRNO_TRANS):
               break
            if (exc.errno in self._SOCK_ERRNO_FATAL):
               raise CloseFD()
            raise
         
         if (0 == rv):
            # Low-level stream file-likes won't do this.
            # ssl.SSLSocket uses this to indicate EAGAIN.
            self._outbuf.appendleft(buf)
            break
         
         if (rv < len(buf)):
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
         if (exc.errno in self._SOCK_ERRNO_TRANS):
            return
         if (exc.errno in self._SOCK_ERRNO_FATAL):
            raise CloseFD()
         raise
      if (br == 0):
         raise CloseFD()
      self._index_in += br
      return br
   
   def getpeercert(self, *args, **kwargs):
      """Return peer certificate for SSL socket."""
      try:
         gpc = self.fl.getpeercert
      except AttributError:
         return None
      
      return gpc(*args, **kwargs)
   
   def _do_ssl_handshake(self, *ssl_args, **ssl_kwargs):
      """Perform SSL handshake, directly. Not for public use; it's racy when
         called from an IO handler for this stream."""
      if (not self):
         # Never mind, then.
         return
      import ssl
      from socket import dup
      
      for bufel in self._outbuf:
         if (hasattr(bufel, 'queue')):
            try:
               raise ValueError("DTR {0!a} in queue; our fd isn't safe to dup.".format(bufel))
            except:
               self.close()
               raise
      
      self.ssl_handshake_pending = None
      self._in = None
      self._out = None
      # Since r80515, ssl.SSLSocket is even harder to deal with than before, since just passing the fileno of an existing
      # connection to the constructor doesn't work correctly anymore.
      # We hack around this API issue here. This now results in two unnecessary dups and closes, but should at least work.
      skwargs = {'fileno': dup(self.fl.fileno())}
      try:
         skwargs['family'] = self.fl.family
         skwargs['type'] = self.fl.type
         skwargs['proto'] = self.fl.proto
      except AttributeError:
         pass
      
      try:
         sock_tmp = socket.socket(**skwargs)
      except:
         os.close(skwargs['fileno'])
         raise
      sock_tmp.setblocking(0)
      self._fw.process_close = lambda: None
      self._fw.close()
      self.fl.close()
      self.fl = None
      try:
         ssl_sock = ssl.SSLSocket(sock=sock_tmp, *ssl_args,
            do_handshake_on_connect=False, **ssl_kwargs)
      finally:
         sock_tmp.close()
      
      ssl_sock.setblocking(0)
      self.fl = ssl_sock
      
      self._fw = self._ed.fd_wrap(ssl_sock.fileno(), fl=ssl_sock)
      self._fw.read_r()
      
      self._fw.process_close = self._process_close
      self._fw.process_writability = self._ssl_handshake_step
      self._fw.process_readability = self._ssl_handshake_step
      self._ssl_handshake_step()
   
   def _ssl_handshake_step(self):
      from ssl import SSLError, SSL_ERROR_WANT_READ, SSL_ERROR_WANT_WRITE
      try:
         self.fl.do_handshake()
      except SSLError as exc:
         if (exc.args[0] == SSL_ERROR_WANT_READ):
            self._fw.write_u()
            return
         if (exc.args[0] == SSL_ERROR_WANT_WRITE):
            self._fw.write_r()
            return
         raise
      
      # XXX: Is this safe? The ssl code suggests that it may return 0 because
      # it wants a socket *read* before willing to write more. Does this
      # actually happen in practice?
      # If so, we should write our own wrapper around fl.write() instead.
      self._in = self.fl.recv_into
      self._out = self.fl.send
      
      self._fw.process_writability = self._output_write
      self._fw.process_readability = self._process_input0
      
      self._unblock_output()
      if (self._outbuf):
         self._fw.write_r()
      
      self.ssl_callback()
      self.ssl_callback = True
      
   def do_ssl_handshake(self, callback, *ssl_args, **ssl_kwargs):
      """Perform SSL handshake."""
      # If we don't have the module, better to find that out now.
      import ssl
      
      if not (self.ssl_callback is None):
         raise Exception('SSL handshake requested previously.')
      
      self.ssl_callback = callback
      self._block_output()
      
      if not (self.connected):
         # We'll let the connect handler do this, then.
         self._out = None
         self.ssl_handshake_pending = (ssl_args, ssl_kwargs)
         return
      
      self._ed.set_timer(0, self._do_ssl_handshake, interval_relative=False,
         args=ssl_args, kwargs=ssl_kwargs)
   
   def sock_set_keepalive(self, v):
      """Set keepalive status on wrapped socket."""
      try:
         from socket import SOL_SOCKET, SO_KEEPALIVE
         self.fl.setsockopt(SOL_SOCKET, SO_KEEPALIVE, v)
      except (ImportError, EnvironmentError):
         return False
      return True
   
   def sock_set_keepidle(self, idle, intvl, cnt):
      """Set keepidle status on wrapped socket."""
      try:
         from socket import SOL_TCP, TCP_KEEPIDLE
         self.fl.setsockopt(SOL_TCP, TCP_KEEPIDLE, idle)
         
         from socket import TCP_KEEPINTVL
         self.fl.setsockopt(SOL_TCP, TCP_KEEPINTVL, intvl)
         
         from socket import TCP_KEEPCNT
         self.fl.setsockopt(SOL_TCP, TCP_KEEPCNT, cnt)
      except (ImportError, EnvironmentError):
         return False
      return True
   
   def _process_input0(self):
      """Input processing stage 0: read and buffer bytes"""
      self._read_data()
      if (self._index_in >= self.size_need):
         self._process_input1()
      if (self._index_in >= self._inbuf_size):
         self._inbuf_resize()

   def _process_input1(self):
      """Override in subclass to insert more handlers"""
      self.process_input(memoryview(self._inbuf)[:self._index_in])


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
      index_l = max(0, self._inbuf_index_l-self._ls_maxlen+1)
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
            line_end = min(line_end,i+len(sep))
         
         if (line_end is None):
            break
         self._process_input2(memoryview(self._inbuf)[line_start:line_end])
         line_start = index_l = line_end
      
      if (line_start):
         self.discard_inbuf_data(line_start)
   
   def _process_input2(self, *args, **kwargs):
      self.process_input(*args, **kwargs)


class AsyncPopen(subprocess.Popen):
   """Popen subclass that automatically creates stdio Async*Stream instances
      for stdio streams of subprocess"""
   def __init__(self, ed, *args, stream_factory=AsyncDataStream, **kwargs):
      subprocess.Popen.__init__(self, *args, **kwargs)
      for name in ('stdin', 'stdout', 'stderr'):
         stream = getattr(self,name)
         if (stream is None):
            continue
         setattr(self, name + '_async', stream_factory(ed, stream))


class AsyncSockServer:
   """Asynchronous listening SOCK_STREAM/SOCK_SEQPACKET sockets.
   
   self.connect_process(sock, addressinfo) should be overridden by the instance
     user; it will be called once for each accepted connection.
   """
   def __init__(self, ed, address, *, family:int=AF_INET, proto:int=0,
         type_:int=SOCK_STREAM, backlog:int=16):
      self.sock = socket_cls(family, type_, proto)
      self.sock.setblocking(0)
      self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      self.sock.bind(address)
      self.sock.listen(backlog)
      self._fw = ed.fd_wrap(self.sock.fileno(), fl=self.sock)
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
      def __call__(self, data, *args,**kwargs):
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
   def alsf(ed, stream):
      return AsyncLineStream(ed, stream, inbufsize_start=1, lineseps=(b'\n', b'\x00', b'\n'))
   
   sp = AsyncPopen(ed, ('ping', '127.0.0.1', '-c', '64'), stdout=PIPE, stream_factory=alsf)
   sp.stdout_async.process_input = D1()
   sp.stdout_async.process_close = close_handler
   stream = sp.stdout_async
   # socket testing code; commented out since it needs a suitably chatty remote
   #sock = AsyncLineStream.build_sock_connect(ed, (('192.168.0.10',6667)))
   #sock.process_input = D1()
   #sock.send_data((b'test\nfoo\nbar\n',),flush=False)
   #stream = sock
   ads_out = AsyncDataStream(ed, open(out.fileno(),'wb', buffering=0, closefd=False), read_r=False)
   ads_out.output_encoding = 'ascii'
   
   ed.event_loop()
   sp.kill()

if (__name__ == '__main__'):
   _selftest(sys.stdout)
