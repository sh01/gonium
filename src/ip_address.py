#!/usr/bin/python
#Copyright 2004, 2005, 2006, 2012 Sebastian Hagen
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


import socket
from socket import inet_pton, inet_ntop, AF_INET, AF_INET6
import struct


def ip_address_build(ip_data):
   if (isinstance(ip_data, IPAddressBase)):
      return ip_data
   if (isinstance(ip_data, int)):
      try:
         return IPAddressV4(ip_data)
      except ValueError:
         try:
            return IPAddressV6(ip_data)
         except ValueError:
            pass
   elif (isinstance(ip_data, (str, bytes, bytearray))):
      try:
         return IPAddressV4.fromstring(ip_data)
      except socket.error:
         try:
            return IPAddressV6.fromstring(ip_data)
         except socket.error:
            pass
   else:
      raise TypeError('Invalid type {!a} for argument ip_data of value {!a} (expected numeric or string type).'.format(type(ip_data), ip_data))
   
   raise ValueError('Unable to convert argument {!a} to a v4 or v6 ip address.'.format(ip_data))


class IPAddressBase(object):
   __slots__ = ['ip']
   
   def __init__(self, ip_int):
      if (ip_int < self.ip_minimum):
         raise ValueError('Value {0} for argument ip_int is smaller than {1}.'.format(ip_int, self.ip_minimum))
      elif (ip_int > self.ip_maximum):
         raise ValueError('Value {0} for argument ip_int is greater than {1}.'.format(ip_int, self.ip_maximum))
      self.ip = ip_int
   
   @classmethod
   def fromstring(cls, ip_string):
      self = cls.__new__(cls)
      self.ip = cls._ipintfromstring(ip_string)
      return self
      
   def __hash__(self):
      return hash(self.ip)
      
   
   def __add__(self, other):
      return self.__class__(int(self)+int(other))
      
   def __sub__(self, other):
      return self.__class__(int(self)-int(other))
      
   def __or__(self, other):
      return self.__class__(int(self) | int(other))
      
   def __xor__(self, other):
      return self.__class__(int(self) ^ int(other))
      
   def __and__(self, other):
      return self.__class__(int(self) & int(other))
      
   __radd__ = __add__
   __rsub__ = __sub__
   __ror__ = __or__
   __rxor__ = __xor__
   __rand__ = __and__
      
   def __not__(self):
      return self.__class__(~int(self))
      
   def __lshift__(self, other):
      return self.__class__(int(self) << other)
   
   def __rshift__(self, other):
      return self.__class__(int(self) >> other)
      
   def __nonzero__(self):
      return bool(self.ip)
      
   def __cmp__(self, other):
      (self, other) = (int(self), int(other))
      if (self < other):
         return -1
      elif (other < self):
         return 1
      else:
         return 0

   def __repr__(self):
      return '{0}.fromstring({1!a})'.format(self.__class__.__name__, self.__str__())
   
   def __int__(self):
      return self.ip
   
   def __getstate__(self):
      return (self.ip,)

   def __setstate__(self, state):
      self.ip = state[0]


class IPAddressV4(IPAddressBase):
   factor = 256
   subelements = 4
   ip_minimum = 0
   ip_maximum = factor**subelements - 1
   AF = AF_INET
   
   @classmethod
   def _ipintfromstring(cls, ip_string):
      if not (isinstance(ip_string, str)):
         # Having to do this is *so* moronic!
         ip_string = ip_string.decode('ascii')
      return struct.unpack(b'>L', inet_pton(cls.AF, ip_string))[0]
   
   def __str__(self):
      return inet_ntop(AF_INET,struct.pack(b'>L', self.ip))


class IPAddressV6(IPAddressBase):
   factor = 65536
   subelements = 8
   ip_minimum = 0
   ip_maximum = factor**subelements - 1
   AF = AF_INET6
   
   @classmethod
   def _ipintfromstring(cls, ip_string):
      if not (isinstance(ip_string, str)):
         # Having to do this is *so* moronic!
         ip_string = ip_string.decode('ascii')
      (int1, int2) = struct.unpack(b'>QQ', inet_pton(cls.AF, ip_string))
      return (int1 << 64) + int2 

   def __str__(self):
      return inet_ntop(AF_INET6, struct.pack(b'>QQ', self.ip >> 64, self.ip & 18446744073709551615)) # 18446744073709551615 == (1 << 64)-1

