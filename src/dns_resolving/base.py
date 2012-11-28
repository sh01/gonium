#!/usr/bin/env python
#Copyright 2007,2008,2012 Sebastian Hagen
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
import struct

from ..ip_address import IPAddressV4, IPAddressV6

CLASS_IN = 1
CLASS_CS = 2
CLASS_CH = 3
CLASS_HS = 4

QTYPE_AXFR = 252
QTYPE_MAILB = 253
QTYPE_MAILA = 254
QTYPE_ALL = 255

QTYPES_SPECIAL = set((QTYPE_AXFR, QTYPE_MAILB, QTYPE_MAILA, QTYPE_ALL))


class DomainName(bytes):
   NAME_LENGTH_LIMIT = 255
   LABEL_LENGTH_LIMIT = 63
   COMP_DEPTH_LIMIT = 256
   def __init__(self, *args, **kwargs):
      bytes.__init__(self)
      if (len(self) > self.NAME_LENGTH_LIMIT):
         raise ValueError('{!a} is longer than {!a}, which is the maximum length allowed for domain names'.format(self, self.NAME_LENGTH_LIMIT))
      self.binstring = self.__binary_repr_compute()
   
   @classmethod
   def build_from_binstream(cls, binstream):
      """Build domain name from binary representation in DNS protocol"""
      elements = []
      stream_pos = None
      comp_depth = 0
      try:
         while (True):
            l_str = binstream.read(1)
            if (l_str == b''):
               raise ValueError('Insufficient data in binstream')
         
            (l,) = struct.unpack(b'>B', l_str)
            if (l > cls.LABEL_LENGTH_LIMIT):
               if (l >= 192):
                  offset = l - 192
                  offset *= 256
                  l_str2 = binstream.read(1)
                  if (l_str2 == b''):
                     raise ValueError('Insufficient data in binstream: missing second byte of compression offset')
                  (l2,) = struct.unpack(b'>B', l_str2)
                  offset += l2
                  
                  comp_depth += 1
                  if (comp_depth > cls.COMP_DEPTH_LIMIT):
                     if (not stream_pos is None):
                        binstream.seek(stream_pos)
                     raise ValueError('Compression depth limit {} exceeded.'.format(cls.COMP_DEPTH_LIMIT))
                  if (stream_pos is None):
                     stream_pos = binstream.tell()
                  binstream.seek(offset)
                  continue
               raise ValueError('Label length {} in stream is greater than allowed maximum {}.'.format(l, cls.LABEL_LENGTH_LIMIT))
         
            label = binstream.read(l)
            if (len(label) != l):
               raise ValueError('Insufficient data in stream for label of length {}.'.format(l,))
            elements.append(label)
            if (len(label) == 0):
               break
      finally:
         if (not stream_pos is None):
            binstream.seek(stream_pos)
      
      if (len(elements) < 1):
         raise ValueError('Insufficient labels in binstream.')
      if (not (elements[-1] == b'')):
         raise ValueError('Terminating label of binstream has non-zero length.')
      del(elements[-1])
      
      if (b'' in elements):
         raise ValueError('Binstream contains empty label before last label.')
      
      return cls(b'.'.join(elements))
      
   def __binary_repr_compute(self):
      """Compute and return binary representation of this domain name in DNS protocol"""
      elements = self.split(b'.')
      if (elements[-1] == b''):
         del(elements[-1])
      
      if (b'' in elements):
         raise ValueError('Empty label in {}'.format(self,))
      
      elements.append(b'')
      
      for element in elements:
         if not (len(element) <= self.LABEL_LENGTH_LIMIT):
            raise ValueError('Element {} of name {} is longer than 63 octets'.format(element,name))
      
      rv = b''.join(((struct.pack(b'>B', len(e)) + e) for e in elements))
      return rv
      
   def binary_repr(self):
      """Return binary representation of this domain name in DNS protocol"""
      return self.binstring

   def __cmp__(self, other):
      """Case-insensitive comparison as manadated by RFC 1035"""
      s1 = str(self.lower())
      s2 = str(other.lower())
      if (s1 < s2):
         return -1
      if (s1 > s2):
         return 1
      return 0


class DNSReprBase(object):
   def __repr__(self):
      return '{}({})'.format(self.__class__.__name__, ', '.join(['{}={!a}'.format(name,getattr(self,name)) for name in self.fields]))

   def __eq__(self, other):
      return (self.binary_repr() == other.binary_repr())
   
   def __neq__(self, other):
      return (not (self == other))

# ----------------------------------------------------------------------------- RDATA sections

class RDATA(DNSReprBase):
   RDATA_TYPES = {}
   fields = ('rdata',)
   def __init__(self, *args, **kwargs):
      fields = list(self.fields)[:]
      for key in kwargs:
         if not (key in fields):
            raise TypeError('Unexpected keyword argument {!a}'.format(key))
         fields.remove(key)
         setattr(self, key, kwargs[key])
      
      if (len(fields) != len(args)):
         raise TypeError('Got {} non-keyword arguments, expected {} (given {} keyword arguments).'.format(len(args), len(fields), len(kwargs)))
      
      for i in range(len(fields)):
         setattr(self, fields[i], args[i])
   
   def data_get(self):
      """Return local representation of data stored in this instace"""
      if (len(self.fields) == 1):
         return getattr(self, self.fields[0])
      
      return tuple([getattr(self, attr) for attr in self.fields])
   
   def binary_repr(self):
      """Return binary representation of this RDATA in DNS protocol"""
      return self.rdata
   
   @staticmethod
   def rdata_read(binstream, rdlength):
      rdata = binstream.read(rdlength)
      if (len(rdata) < rdlength):
         raise ValueError('Insufficient data in binstream')
      
      return rdata

   @classmethod
   def build_from_binstream(cls, *args, **kwargs):
      return cls(cls.rdata_read(*args, **kwargs))

   @classmethod
   def rdata_type_register(cls, rdata_type):
      cls.RDATA_TYPES[rdata_type.type] = rdata_type
      return rdata_type
   
   @classmethod
   def class_get(cls, rtype):
      if (rtype in cls.RDATA_TYPES):
         return cls.RDATA_TYPES[rtype]
      return RDATA


@RDATA.rdata_type_register
class RDATA_A(RDATA):
   type = 1
   fields = ('ip',)
   def binary_repr(self):
      """Return binary representation of this RDATA in DNS protocol"""
      return struct.pack('>I', int(self.ip))
   
   @classmethod
   def build_from_binstream(cls, binstream, rdlength):
      rdata = cls.rdata_read(binstream, rdlength)
      if (len(rdata) != 4):
         raise ValueError('Rdata {!a} has invalid length; expected 4 octets.'.format(rdata,))
      ip = IPAddressV4(struct.unpack('>I', rdata)[0])
      return cls(ip)


class RDATA_DomainName(RDATA):
   fields = ('domain_name',)
   def binary_repr(self):
      """Return binary representation of this RDATA in DNS protocol"""
      return DomainName(self.domain_name).binary_repr()
   
   @classmethod
   def build_from_binstream(cls, binstream, rdlength):
      pos = binstream.tell()
      rdata = cls.rdata_read(binstream, rdlength)
      pos_end = binstream.tell()
      binstream.seek(pos)
      domain_name = DomainName.build_from_binstream(binstream)
      if not (pos_end == binstream.tell()):
         raise ValueError('Rdata {!a} is not a valid domain name.'.format(rdata,))
      return cls(domain_name)


@RDATA.rdata_type_register
class RDATA_NS(RDATA_DomainName):
   type = 2

@RDATA.rdata_type_register
class RDATA_CNAME(RDATA_DomainName):
   type = 5

@RDATA.rdata_type_register
class RDATA_SOA(RDATA):
   type = 6
   fields = ('mname', 'rname', 'serial', 'refresh', 'retry', 'expire', 'minimum')
   def binary_repr(self):
      """Return binary representation of this RDATA in DNS protocol"""
      mname_str = DomainName(self.mname).binary_repr()
      rname_str = DomainName(self.rname).binary_repr()
      return ('%s%s%s' % (mname_str, rname_str, struct.pack('>IIIII', self.serial, 
         self.refresh, self.retry, self.expire, self.minimum)))
   
   @classmethod
   def build_from_binstream(cls, binstream, rdlength):
      pos = binstream.tell()
      rdata = cls.rdata_read(binstream, rdlength)
      pos_end = binstream.tell()
      binstream.seek(pos)
      mname = DomainName.build_from_binstream(binstream)
      rname = DomainName.build_from_binstream(binstream)
      srrem_str = binstream.read(20)
      if (len(srrem_str) != 20):
         raise ValueError('Rdata {!a} is not a valid SOA record'.format(rdata,))
      
      if not (pos_end == binstream.tell()):
         raise ValueError('Rdata {!a} is not a valid SOA record.'.format(rdata,))
      
      
      (serial, refresh, retry, expire, minimum) = \
         struct.unpack('>IIIII', srrem_str)
      
      return cls(mname, rname, serial, refresh, retry, expire, minimum)

@RDATA.rdata_type_register
class RDATA_PTR(RDATA_DomainName):
   type = 12

@RDATA.rdata_type_register
class RDATA_MX(RDATA):
   type = 15
   fields = ('preference', 'hostname')
   def binary_repr(self):
      """Return binary representation of this RDATA in DNS protocol"""
      return ('%s%s' % (struct.pack('>H', self.preference),
         DomainName(self.hostname).binary_repr()))
   
   @classmethod
   def build_from_binstream(cls, binstream, rdlength):
      pos = binstream.tell()
      rdata = cls.rdata_read(binstream, rdlength)
      pos_end = binstream.tell()
      binstream.seek(pos)
      pref_str = binstream.read(2)
      if (len(pref_str) < 2):
         raise ValueError('Rdata {!a} is not a valid MX record.'.format(rdata,))
      
      hostname = DomainName.build_from_binstream(binstream)
      if not (pos_end == binstream.tell()):
         raise ValueError('Rdata {!a} is not a valid MX record.'.format(rdata,))
      
      (preference,) = struct.unpack('>H', pref_str)
      return cls(preference, hostname)

@RDATA.rdata_type_register
class RDATA_TXT(RDATA):
   type = 16
   fields = ('txt_data',)
   def __init__(self, txt_data):
      self.txt_data = txt_data
      for txt in txt_data:
         if (len(txt) > 255):
            raise ValueError('TXT fragment {!a} is too long (maximum is 255 bytes).'.format(txt))

   def binary_repr(self):
      """Return binary representation of this RDATA in DNS protocol"""
      for txt in self.txt_data:
         if (len(txt) > 255):
            raise ValueError('Txt fragment {!a} is too long.'.format(txt,))
      return b''.join((struct.pack(b'>B', len(txt)) + txt) for txt in
         self.txt_data)
   
   @classmethod
   def build_from_binstream(cls, binstream, rdlength):
      rdata = cls.rdata_read(binstream, rdlength)
      i = 0
      txt_data = []
      while (i < rdlength):
         (slen,) = struct.unpack(b'>B', rdata[i:i+1])
         j = i + 1 + slen
         txt = rdata[i+1:j]
         if (len(txt) < slen):
            raise ValueError('Rdata {!a} is not a valid TXT record'.format(rdata,))
         txt_data.append(txt)
         i = j
      
      return cls(tuple(txt_data))

# RFC 3596
@RDATA.rdata_type_register
class RDATA_AAAA(RDATA):
   type = 28
   fields = ('ip',)
   def binary_repr(self):
      """Return binary representation of this RDATA in DNS protocol"""
      return socket.inet_pton(socket.AF_INET6, str(self.ip))
   
   @classmethod
   def build_from_binstream(cls, binstream, rdlength):
      if (rdlength != 16):
         raise ValueError('Length of AAAA record must be 16 octets; got {!a}.'.format(rdlength,))
      rdata = cls.rdata_read(binstream, rdlength)
      ip = IPAddressV6.fromstring(socket.inet_ntop(socket.AF_INET6, rdata))
      return cls(ip)

QTYPE_A = RDATA_A.type
QTYPE_AAAA = RDATA_AAAA.type