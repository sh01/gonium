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

# FDM classes for handling packet sequences (e.g. on DGRAM sockets).

from errno import EAGAIN
from socket import error as sockerr, socket as socket_


class AsyncPacketSock:
   """Interface for asynchronously interfacing with DGRAM-like socks
   
   public attributes (read-only):
     fl: wrapped filelike
     bufsize: buffer size passed to recvfrom()
   public attributes (rw):
     process_input(data, addrinfo): handler for read datagrams
   """
   def __init__(self, ed, filelike, *, read_r:bool=True, bufsize=65536):
      self._ed = ed
      self.fl = fileike
      self._fw = ed.fd_wrap(fl.fileno())
      self._fw.readability_process = self._process_input0
      if (read_r):
         self._fw.read_r()
      self.bufsize = bufsize
   
   def _process_input0(self):
      while (True):
         try:
            (data, addrinfo) = self.fl.recvfrom(self.bufsize)
         except sockerr as exc:
            if (sockerr.errno != EAGAIN):
               raise
            break
         self.process_input(data, addrinfo)

