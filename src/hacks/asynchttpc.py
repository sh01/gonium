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

"""Hacks for forcing httplib-using modules into pseudo-asynchronous operation."""


# This is a dirty hack to compensate for the fact that we need HTTP support,
# don't want to implement it ourselves, and the python libs are intended
# strictly for blocking usage, which we don't want either.
# While the point of this is to reduce the necessity for threads, the code
# itself should also be threadsafe.

# Warning: the following code may be offensive to some readers.
# Then again, using threads just to work around unnecessary library limitations
# is slightly offensive to the author.

import os
import sys
import socket
import logging
import http.client
import urllib.request
from io import BytesIO
from http.client import HTTPConnection as HTTPConnection_orig
from threading import Lock

from ..fdm.stream import AsyncDataStream

class HTTPHacksError(Exception):
   pass

class HTTPHacksLockingError(HTTPHacksError):
   pass

class HTTPHacksNoResponseSetError(HTTPHacksError):
   """An exception that also happens to deliver request data."""
   def __init__(self, host, port, req_string, *args, **kwargs):
      self.req_string = req_string
      self.host = host
      self.port = port
      HTTPHacksError.__init__(self, *args, **kwargs)

class HTTPHacksStateError(HTTPHacksError):
   pass

class HttpHacksFakeSock:
   def __init__(self, file):
      self.file = file
   def makefile(self, mode, bufsize):
      """Return stored file-like object. Parameters are ignored."""
      return self.file

class HTTPHacksMaximumQueryDepthExceededError(HTTPHacksError):
   pass


class HTTPConnectionData:
   def __init__(self, host, port, req_string):
      self.host = host
      self.port = port
      self.req_string = req_string
   
   def __repr__(self):
      return '%r(%r, %r, %r)' % (self.__class__.__name__, self.host, self.port, self.req_string)

class HTTPConnection(HTTPConnection_orig):
   """Return input using an exception, or alternatively provide output to caller."""
   _hack_logger = logging.getLogger('HTTPConnection')
   _hack_log = _hack_logger.log
   responses = ()
   lock = Lock()
   def __init__(self, host, port=None, *args, **kwargs):
      if (port is None):
         port = 80
      HTTPConnection_orig.__init__(self, host, port *args, **kwargs)
      self.req_string = b''
      self.__host = host
      self.__port = port
   
   def send(self, string):
      self.req_string += string
      
   def getresponse(self):
      if not (self.responses):
         # the original caller responsible for activating the hack should catch
         # this, and extract the saved req_string.
         raise HTTPHacksNoResponseSetError(self.__host, self.__port,
               self.req_string, 'Out of responses.')
      
      response_str = self.responses.pop(0)
      self._hack_log(10, 'Returning response {0}.'.format(response_str,))
      rv = self.response_class(HttpHacksFakeSock(BytesIO(response_str)))
      rv.begin()
      
      return rv

   @classmethod
   def hack_enable(cls, responses=(), lock_wait=False):
      """Enable the hack.
      
      This acquires a class-level lock. Be sure to call hack_disable() once you are done."""
      cls._hack_log(12, 'Acquiring lock for http_hack and activating.')
      cls.responses = list(responses)
      lock_rv = cls.lock.acquire(lock_wait)
      if not (lock_wait or lock_rv):
         raise HTTPHacksLockingError('Non-blocking httphacks lock acquiration failed.')
      http.client.HTTPConnection = cls

   @classmethod
   def hack_disable(cls):
      """Disable the hack and release class-level lock"""
      cls._hack_log(12, 'Releasing lock for http_hack and deactivating.')
      cls.lock.release()
      http.client.HTTPConnection = HTTPConnection_orig
      
   @classmethod
   def call_wrap(cls, callee, call_args, call_kwargs, responses=(), lock_wait=False):
      """Activate hack, call callee, disable hack and return result
      
      This calls callee(*call_args, **call_kwargs) while the hack is active.
      This returns a (<caught_exc>, val) pair.
      Val is the req_string if an HTTPHacksNoResponseSetError is caught, and
      callee's return value if callee doesn't throw an exception."""

      cls.hack_enable(responses=responses, lock_wait=lock_wait)
      try:
         try:
            rv = (False, callee(*call_args, **call_kwargs))
         except HTTPHacksNoResponseSetError as exc:
            rv = (True, HTTPConnectionData(exc.host, exc.port, exc.req_string))
      finally:
         cls.hack_disable()

      return rv
   

class HTTPFetcher:
   """Fetch a file over http using gonium event-loop"""
   logger = logging.getLogger('HTTPFetcher')
   log = logger.log
   init_args = ('ed', 'url', 'ua', 'timeout', 'query_depth_limit')
   def __init__(self, ed, url, ua='python...urllib_two_...spoofed because some people block it...',
         timeout=100, query_depth_limit=10):
      self.ed = ed
      self.url = url
      self.ua = ua
      self.target_host = None
      self.target_port = None
      self.req_string = None
      self.req_connection = None
      self.req_timer = None
      self.targets = []
      self.responses = []
      self.timeout = timeout
      self.error_abort = False
      self.query_depth_limit = query_depth_limit
      self.query_depth = 0
   
   # subclass interface
   def result_process(self, urlo):
      """Process http fetch result; not implemented here"""
      raise NotImplementedError()

   def abort_depth_process(self):
      """Process http fetch abortion because of exceeded query depth; not implemented here"""
      raise NotImplementedError()

   def abort_query_process(self, exc=None):
      """Process http fetch abortion because of failure/timeout on individual query; not implemented here"""
      raise NotImplementedError()

   # implemented methods
   def sub_input_handler(self):
      """Handle input from connection"""
      pass
   
   def sub_close_handler(self, fd):
      """Handle closing of connection"""
      conn = self.req_connection
      self.req_connection = None
      if (self.error_abort):
         return
      self.req_timer.stop()
      self.req_timer = None
      self.responses.append(conn.buffers_input.values()[0])
      self.query_depth += 1
      if (self.query_depth >= self.query_depth_limit):
         self.abort_depth_process()
      else:
         self.query_init()
   
   def sub_timeout_handler(self):
      """Handle timeout while retrieving data"""
      self.log(30, 'Query of {0!a} to {1!a} timeouted.'.format(self, self.targets[-1]))
      self.error_abort = True
      try:
         self.req_timer.stop()
      except ValueError:
         sys.exc_clear()
      
      self.req_timer = None
      self.req_connection.close()
      self.req_connection = None
      self.error_abort = False
      self.abort_query_process(None)
      
   def query_init(self):
      """Process received data, and optionally start new query"""
      if (self.req_connection):
         return HTTPHacksStateError('Already retrieving data')
      req_headers = {}
      if (self.ua):
         req_headers['User-Agent'] = self.ua
      ul_req = urllib.request.Request(self.url, None, req_headers)
      try:
         (got_exc, data) = HTTPConnection.call_wrap(urllib.request.urlopen, (ul_req,), {}, tuple(self.responses))
      except (Exception, http.client.HTTPException) as exc:
         self.log(40, 'urllib.request.urlopen call failed:', exc_info=True)
         self.abort_query_process(exc)
         return
      if not (got_exc is True):
         self.log(20, '{0!a} finished retrieving data to url {1!a}.'.format(self, self.url))
         self.result_process(data)
         return
      self.target_host = data.host
      self.target_port = data.port
      self.req_string = data.req_string
      
      target = (self.target_host, self.target_port)
      self.log(14, '{0!a} starting query to {1!a}.'.format(self, target))
      
      def connect_process():
         self.req_connection.send_data(self.req_string)
      
      try:
         self.req_connection = AsyncDataStream.build_sock_connect(self.ed, target, connect_process)
      except (Exception,socket.error) as exc:
         self.abort_query_process(exc)
         return

      self.req_connection.process_input = self.sub_input_handler
      self.req_connection.process_close = self.sub_close_handler
      self.targets.append(target)
      self.req_timer = Timer(self.ed, self.timeout, self.sub_timeout_handler)

   def __repr__(self):
      return '{0!s}{0!a}'.format(self.__class__.__name__, tuple([getattr(self, name) for name in self.init_args]))


class SimpleHTTPFetcher(HTTPFetcher):
   """Fetch a file over http using gonium event-loop and deferring processing to
      parent instance"""
   init_args = ('parent', 'ed', 'url', 'ua', 'timeout', 'query_depth_limit')
   def __init__(self, ed, parent, *args, **kwargs):
      self.parent = parent
      HTTPFetcher.__init__(self, ed, *args, **kwargs)
   
   def result_process(self, urlo):
      """Process http fetch result"""
      self.parent.http_result_handle(self, urlo)

   def abort_depth_process(self):
      """Process http fetch abortion because of exceeded query depth"""
      exc = HTTPHacksMaximumQueryDepthExceededError('{0!a} exceeded maximum'
         'query depth after connecting to ({1!a},{2!a}) and using reqstring'
         '{3!a}.'.format(self, self.target_host, self.target_port, self.req_string))
      self.parent.http_failure_handle(self, exc)

   def abort_query_process(self, exc=None):
      """Process http fetch abortion because of failure/timeout on individual query"""
      self.parent.http_failure_handle(self, exc)


def _selftest():
   # Test and demonstration code
   import sys, socket, urllib.request
   logger = logging.getLogger()
   log = logger.log
   logger.setLevel(logging.DEBUG)
   formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
   handler_stderr = logging.StreamHandler()
   handler_stderr.setLevel(logging.DEBUG)
   handler_stderr.setFormatter(formatter)
   logger.addHandler(handler_stderr)

   log(20, 'Demonstraction mode for hacks.aynchttpc activated.')
   
   url = sys.argv[1]
   log(20, 'Target url determined: {0!a}'.format(url))
   netloc = urllib.request.urlparse(url)[1]
   host = netloc
   
   log(20, 'Target host is {0!a}.'.format(host,))
   
   log(20, 'Getting req string.')
   (got_exc, hcd) = HTTPConnection.call_wrap(urllib.request.urlopen, (url,), {}, ())
   if not (got_exc):
      raise Exception('First hacks.aynchttpc call failed. Results: {0!a}'.format((got_exc, hcd),))
   
   req_string = hcd.req_string
   log(20, 'Req string is {0!a}.'.format(req_string))
   
   s = socket.socket()
   log(20, 'Connecting to {0!a}.'.format(host,))
   s.connect((netloc, 80))
   s.setblocking(1)
   log(20, 'Done. Sending query.')
   s.sendall(req_string)
   log(20, 'Done. Reading response.')
   str_response = s.recv(100000000)
   s.close()
   log(20, 'Done. Response is: {0!a}. Preparing to pass response back through urllib.request for parsing.'.format(str_response,))
   
   (got_exc, urlo) = HTTPConnection.call_wrap(urllib.request.urlopen, (url,), {}, (str_response,))
   if (got_exc):
      raise Exception('Second hacks.aynchttpc call failed. URL attempted additional query: {0!a}'.format(urlo,))
   log(20, 'Done. Got urlo instance {0!a}.'.format(urlo,))
   
   str_response_content = urlo.read()
   
   log(20, 'Extracted content is {0!a}.'.format(str_response_content,))
   
   log(20, 'All done.')

if (__name__ == '__main__'):
   _selftest()
