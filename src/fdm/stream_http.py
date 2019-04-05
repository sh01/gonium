#!/usr/bin/env python
#Copyright 2019 Sebastian Hagen
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

from .stream import AsyncDataStream


def _local_from_urlsplit(us):
  p = us.path
  if (len(p) < 1):
    p = b'/'
  f = [p]
  if (us.query):
    f.append(b'?')
    f.append(us.query)
  if (us.fragment):
    f.append(b'#')
    f.append(us.fragment)
  return b''.join(f)

def _urlsplit2hostport(us):
  f = us.netloc.rsplit(b':')
  host = f[0]
  if (len(f) > 1):
    port = int(f[1])
  elif (us.scheme == b'http'):
    port = 80
  elif (us.scheme == b'https'):
    port = 443
  else:
    raise Error('Unable to determine port from netloc/scheme {!a}/{!a}.'.format(us.netloc, us.scheme))
  return (host, port)
    
_EOL = b'\x0d\x0a'
class Query:
  PROTO_VER = b'HTTP/1.1'
  def __init__(self, url, method=b'GET', hdrs={}):
    from urllib.parse import urlsplit
    if isinstance(url, str):
      url = url.encode('ascii')
    self.url = url
    self.method = method
    self.hdrs = hdrs

    us = urlsplit(url)
    self.lp = _local_from_urlsplit(us)
    (self.host, self.port) = _urlsplit2hostport(us)
    self.is_ssl = (us.scheme == b'https')

  def build_req(self):
    f = [
      self.method, b' ', self.lp, b' ', self.PROTO_VER, _EOL,
      b'Host: ', self.host, _EOL,
      b'Connection: close', _EOL,
    ]
    for k,v in self.hdrs.items():
      f.extend([k, b': ', v, _EOL])
    f.append(_EOL)
    return b''.join(f)

class AsyncHTTPStream(AsyncDataStream):
  """
  Public attributes (r/w):
     process_input(data): See parent. Payload data only.
     process_close(): See parent.
     process_hdrs(self, scode, hdrs): Process initial HTTP response.
  """
  def __init__(self, *args, **kwargs):
    self.__hdr_offset = 0
    super().__init__(*args, **kwargs)

  def send_request(self, q):
    self.send_bytes((q.build_req(),))

  def _process_input1(self):
    # Find end of HTTP headers.
    off = bytes(self._inbuf[self.__hdr_offset:self._index_in]).find(b'\x0d\x0a\x0d\x0a')
    if off < 0:
      self.__hdr_offset = max(self._index_in-3, 0)
      return
    off += self.__hdr_offset
    hdr_data = bytes(self._inbuf[:off])
    self.discard_inbuf_data(off+4)
    # Parse HTTP header data.
    hdr_split = hdr_data.split(b'\x0d\x0a')
    status_code = int(hdr_split[0].split()[1])
    hdrs = {}
    for line in hdr_split[1:]:
      (k, v) = line.split(b':', 1)
      hdrs[k.lower().strip()] = v.lstrip()
    self.process_hdrs(status_code, hdrs)
    # Restore non-header handler
    s = super()._process_input1
    self._process_input1 = s
    # We have some initial payload data; call payload handler now.
    if (self._index_in > 0):
      s()
    
  @classmethod
  def build_by_query(cls, sa, query):
    self = cls(sa.ed, run_start=False)

    def connect_cb(*args):
      self.send_request(query)
    
    if (query.is_ssl):
      self.do_ssl_handshake(connect_cb)
      connect_cb = None

    def start():
      self.connect_async_sock_bydns(sa, query.host, query.port, connect_callback=connect_cb)
    return self, start


def _selftest():
  import sys
  from ..service_aggregation import ServiceAggregate

  class T(AsyncHTTPStream):
    def process_input(self, data):
      print(bytes(data))
    def process_hdrs(self, scode, hdrs):
      print('Status: {}'.format(scode))
      from pprint import pprint
      pprint(hdrs)
    def process_close(self):
      print('Connection closed.')
      sa.ed.shutdown()
  
  out = sys.stdout
  url = sys.argv[1]
  sa = ServiceAggregate()
  sa.add_dnslm()
  ed = sa.ed

  out.write('Using ED {}\n'.format(ed))
  out.write('==== AsyncHTTPStream test ====\n')

  q = Query(url.encode('ascii'))
  s, start = T.build_by_query(sa, q)
  start()
  sa.ed.event_loop()

if __name__ == '__main__':
  _selftest()
