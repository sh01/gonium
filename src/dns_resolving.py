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

import logging
import struct
import socket
from io import BytesIO
import random

from . import ip_address
from .ip_address import IPAddressV4, IPAddressV6
from .fdm import AsyncPacketSock, Timer

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
         raise ValueError('{0!a} is longer than {0!a}, which is the maximum'
        'length allowed for domain names'.format(self, self.NAME_LENGTH_LIMIT))
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
                     raise ValueError('Compression depth limit {0} exceeded.'.format(cls.COMP_DEPTH_LIMIT))
                  if (stream_pos is None):
                     stream_pos = binstream.tell()
                  binstream.seek(offset)
                  continue
               raise ValueError('Label length {0} in stream is greater than allowed maximum {1}.'.format(l, cls.LABEL_LENGTH_LIMIT))
         
            label = binstream.read(l)
            if (len(label) != l):
               raise ValueError('Insufficient data in stream for label of length {0}.'.format(l,))
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
         raise ValueError('Empty label in {0}'.format(self,))
      
      elements.append(b'')
      
      for element in elements:
         if not (len(element) <= self.LABEL_LENGTH_LIMIT):
            raise ValueError('Element {0} of name {1} is longer than 63 octets'.format(element,name))
      
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
      return '{0}({1})'.format(self.__class__.__name__, ', '.join(['{0}={1!a}'.format(name,getattr(self,name)) for name in self.fields]))

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
            raise TypeError('Unexpected keyword argument {0!a}'.format(key))
         fields.remove(key)
         setattr(self, key, kwargs[key])
      
      if (len(fields) != len(args)):
         raise TypeError('Got {0} non-keyword arguments, expected {1} (given'
         '{2} keyword arguments).'.format(len(args), len(fields), len(kwargs)))
      
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
         raise ValueError('Rdata {0!a} has invalid length; expected 4 octets.'.format(rdata,))
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
         raise ValueError('Rdata {0!a} is not a valid domain name.'.format(rdata,))
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
         raise ValueError('Rdata {0!a} is not a valid SOA record'.format(rdata,))
      
      if not (pos_end == binstream.tell()):
         raise ValueError('Rdata {0!a} is not a valid SOA record.'.format(rdata,))
      
      
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
         raise ValueError('Rdata {0!a} is not a valid MX record.'.format(rdata,))
      
      hostname = DomainName.build_from_binstream(binstream)
      if not (pos_end == binstream.tell()):
         raise ValueError('Rdata {0!a} is not a valid MX record.'.format(rdata,))
      
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
            raise ValueError('TXT fragment {0!a} is too long (maximum is 255 bytes).'.format(txt))

   def binary_repr(self):
      """Return binary representation of this RDATA in DNS protocol"""
      for txt in self.txt_data:
         if (len(txt) > 255):
            raise ValueError('Txt fragment {0!a} is too long.'.format(txt,))
      return b''.join((struct.pack(b'>B', len(txt)) + txt) for txt in
         self.txt_data)
   
   @classmethod
   def build_from_binstream(self, binstream, rdlength):
      rdata = cls.rdata_read(binstream, rdlength)
      i = 0
      txt_data = []
      while (i < rdlength):
         (slen,) = struct.unpack(b'>B', rdata[i])
         j = i + 1 + slen
         txt = rdata[i+1:j]
         if (len(txt) < slen):
            raise ValueError('Rdata {0!a} is not a valid TXT record'.format(rdata,))
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
         raise ValueError('Length of AAAA record must be 16 octets; got {0!a}.'.format(rdlength,))
      rdata = cls.rdata_read(binstream, rdlength)
      ip = IPAddressV6.fromstring(socket.inet_ntop(socket.AF_INET6, rdata))
      return cls(ip)


# ----------------------------------------------------------------------------- question / RR sections

class ValueVerifier:
   NAME_MIN = 0
   NAME_MAX = 255
   TYPE_MIN = 1
   TYPE_MAX = 255
   @classmethod
   def name_validate(cls, name):
      if not (cls.NAME_MIN <= len(name) <= cls.NAME_MAX):
         raise ValueError('NAME {0!a} is invalid'.format(name,))
   
   @classmethod
   def type_validate(cls, rtype):
      if not (cls.TYPE_MIN <= int(rtype) <= cls.TYPE_MAX):
         raise ValueError('TYPE {0!a} is invalid'.format(rtype,))


class DNSQuestion(ValueVerifier, DNSReprBase):
   fields = ('name', 'type', 'rclass')
   def __init__(self, name, rtype, rclass=CLASS_IN):
      self.name_validate(name)
      self.type_validate(rtype)
      self.name = name
      self.type = rtype
      self.rclass = rclass
   
   @classmethod
   def build_from_binstream(cls, binstream):
      name = DomainName.build_from_binstream(binstream)
      tc_str = binstream.read(4)
      if (len(tc_str) < 4):
         raise ValueError('Insufficient data in binstream')
      
      (rtype, rclass) = struct.unpack(b'>HH', tc_str)
      return cls(name, rtype, rclass)
   
   def binary_repr(self):
      """Return binary representation of this question section"""
      return (self.name.binary_repr() + struct.pack(b'>HH', self.type, 
         self.rclass))
   


class ResourceRecord(ValueVerifier, DNSReprBase):
   fields = ('name', 'type', 'rclass', 'ttl', 'rdata')
   def __init__(self, name, rtype, ttl, rdata, rclass=CLASS_IN):
      self.name_validate(name)
      self.type_validate(rtype)
      self.name = name
      self.type = rtype
      self.rclass = rclass
      self.ttl = ttl
      self.rdata = rdata
   
   @classmethod
   def build_from_binstream(cls, binstream):
      name = DomainName.build_from_binstream(binstream)
      tctl_str = binstream.read(10)
      if (len(tctl_str) < 10):
         raise ValueError('Insufficient data in binstream')
      
      (rtype, rclass, ttl, rdlength) = struct.unpack(b'>HHLH', tctl_str)

      rdata = RDATA.class_get(rtype).build_from_binstream(binstream, rdlength)
      return cls(name, rtype, ttl, rdata, rclass=rclass)

   def binary_repr(self):
      """Return binary representation of this RR"""
      rdata_str = self.rdata.binary_repr()
      return (self.name.binary_repr() + struct.pack(b'>HHLH', self.type,
         self.rclass, self.ttl, len(rdata_str)) + rdata_str)


# ----------------------------------------------------------------------------- DNS Frames and Headers

class DNSHeader(DNSReprBase):
   ID_MIN = QDC_MIN = ANC_MIN = NSC_MIN = ARC_MIN = 0
   ID_MAX = QDC_MAX = ANC_MAX = NSC_MAX = ARC_MAX = 65535
   QR_MIN = AA_MIN = TC_MIN = RD_MIN = RA_MIN = False
   QR_MAX = AA_MAX = TC_MAX = RD_MAX = RA_MAX = True
   OPCODE_MIN = 0
   OPCODE_MAX = 2
   RCODE_MIN = 0
   RCODE_MAX = 5
   
   fields = ('id', 'response', 'opcode', 'authoritative_answer', 'truncation',
      'recursion_desired', 'recursion_available', 'response_code', 'qdcount',
      'ancount', 'nscount', 'arcount')
   
   def __init__(self, id, response=False, opcode=0, authoritative_answer=False,
      truncation=False, recursion_desired=True, recursion_available=False,
      response_code=0, qdcount=0, ancount=0, nscount=0, arcount=0):
      self.limit_verify(self.ID_MIN, self.ID_MAX, id)
      self.limit_verify(self.QR_MIN, self.QR_MAX, response)
      self.limit_verify(self.OPCODE_MIN, self.OPCODE_MAX, opcode)
      self.limit_verify(self.AA_MIN, self.AA_MAX, authoritative_answer)
      self.limit_verify(self.TC_MIN, self.TC_MAX, truncation)
      self.limit_verify(self.RD_MIN, self.RD_MAX, recursion_desired)
      self.limit_verify(self.RA_MIN, self.RA_MAX, recursion_available)
      self.limit_verify(self.RCODE_MIN, self.RCODE_MAX, response_code)
      self.limit_verify(self.QDC_MIN, self.QDC_MAX, qdcount)
      self.limit_verify(self.ANC_MIN, self.ANC_MAX, ancount)
      self.limit_verify(self.NSC_MIN, self.NSC_MAX, nscount)
      self.limit_verify(self.ARC_MIN, self.ARC_MAX, arcount)
      self.id = id
      self.response = response
      self.opcode = opcode
      self.authoritative_answer = authoritative_answer
      self.truncation = truncation
      self.recursion_desired = recursion_desired
      self.recursion_available = recursion_available
      self.response_code = response_code
      self.qdcount = qdcount
      self.ancount = ancount
      self.nscount = nscount
      self.arcount = arcount

   @staticmethod
   def limit_verify(limit_min, limit_max, val):
      if not (limit_min <= val <= limit_max):
         raise ValueError('Expected value to lie between {0} and {1}; got {2}'
                          'instead.'.format(limit_min, limit_max, val))

   @classmethod
   def build_from_binstream(cls, binstream):
      s = binstream.read(12)
      if (len(s) < 12):
         raise ValueError('Insufficient data in stream')
      
      return cls.build_from_binstring(s)
   
   @classmethod
   def build_from_binstring(cls, binstring):
      if (len(binstring) != 12):
         raise ValueError('Binstring {0!a} has invalid length'.format(binstring,))
      
      (id, flags_1, flags_2, qdcount, ancount, nscount, arcount) = \
         struct.unpack(b'>HBBHHHH', binstring)
      
      qr = bool(flags_1 >> 7)
      opcode = (flags_1 % 128) >> 3
      aa = bool((flags_1 % 8) >> 2)
      tc = bool((flags_1 % 4) >> 1)
      rd = bool(flags_1 % 2)
      
      ra = bool(flags_2 >> 7)
      Z = (flags_2 % 128) >> 4
      rcode = flags_2 % 16
      
      if (Z != 0):
         raise ValueError('Got non-zero value in Z header field')
      
      return cls(id, qr, opcode, aa, tc, rd, ra, rcode, qdcount,
         ancount, nscount, arcount)

   def binary_repr(self):
      """Return binary representation of this DNS Header"""
      flags_1 = (
         (self.response << 7) +
         (self.opcode << 3) +
         (self.truncation << 2) +
         (self.recursion_desired)
      )
      
      flags_2 = (self.recursion_available << 7) + self.response_code
      
      return struct.pack(b'>HBBHHHH', self.id, flags_1, flags_2, self.qdcount,
         self.ancount, self.nscount, self.arcount)


class DNSFrame(DNSReprBase):
   fields = ('questions', 'answers', 'ns_records', 'ar', 'header')
   def __init__(self, questions, answers=(), ns_records=(), 
         ar=(), header=None, *args, **kwargs):
      self.questions = questions
      self.answers = answers
      self.ns_records = ns_records
      self.ar = ar
      if (header is None):
         header = DNSHeader(qdcount=len(questions), ancount=len(answers),
         nscount=len(ns_records), arcount=len(ar), *args, **kwargs)
      
      self.header = header

   @classmethod
   def build_from_binstream(cls, binstream):
      header = DNSHeader.build_from_binstream(binstream)
      questions = tuple([DNSQuestion.build_from_binstream(binstream) for i in range(header.qdcount)])
      answers = tuple([ResourceRecord.build_from_binstream(binstream) for i in range(header.ancount)])
      ns_records = tuple([ResourceRecord.build_from_binstream(binstream) for i in range(header.nscount)])
      ar = tuple([ResourceRecord.build_from_binstream(binstream) for i in range(header.arcount)])
      
      return cls(header=header, questions=questions, answers=answers,
         ns_records=ns_records, ar=ar)

   def binary_repr(self):
      """Return binary representation of this DNS Header"""
      return (self.header.binary_repr() +
         b''.join([s.binary_repr() for s in (tuple(self.questions) +
         tuple(self.answers) + tuple(self.ns_records) + 
         tuple(self.ar))]))


# ----------------------------------------------------------------------------- statekeeping

class DNSQuery:
   """Class representing outstanding local requests
   
   Callback args: dns_request, response_frame
   response_frame will be None iff the query timeouted."""
   def __init__(self, lookup_manager, result_handler, id, question, timeout):
      if (not hasattr(question.binary_repr,'__call__')):
         raise ValueError('Value {0!a} for argument question is invalid'.format(question,))
      
      self.id = id
      self.result_handler = result_handler
      self.question = question
      self.la = lookup_manager
      self.la.query_add(self)
      if not (timeout is None):
         self.tt = Timer(self.la.event_dispatcher, timeout, self.timeout_process, parent=self)
   
   def timeout_process(self):
      """Process a timeout on this query"""
      self.tt = None
      self.la.query_forget(self)
      try:
         self.failure_report()
      finally:
         self.la = None
   
   def failure_report(self):
      """Call callback handler with dummy results indicating lookup failure"""
      self.result_handler(self, None)
   
   def potential_response_process(self, response):
      """Check a response for whether it answers our query, and if so process it.
      
      Returns whether the response was accepted."""
      if (tuple(response.questions) != (self.question,)):
         return False
      
      self.tt.cancel()
      self.tt = None
      try:
         self.result_handler(self, response)
      finally:
         self.la = None
      
      return True
   
   def clean_up(self):
      """Cancel request, if still pending"""
      if not (self.tt is None):
         self.tt.cancel()
         self.tt = None
      if not (self.la is None):
         self.la.query_forget(self)
         self.la = None


class DNSLookupManager:
   logger = logging.getLogger('gonium.dns_resolving.DNSLookupManager')
   log = logger.log
   def __init__(self, event_dispatcher, ns_addr, addr_family=socket.AF_INET):
      self.event_dispatcher = event_dispatcher
      
      sock = socket.socket(addr_family, socket.SOCK_DGRAM)
      self.sock = AsyncPacketSock(self.event_dispatcher, sock)
      self.sock.process_input = self.data_process
      self.sock.process_close = self.close_process

      self.cleaning_up = False
      if (not (len(ns_addr) == 2)):
         raise ValueError('Argument ns_addr should have two elements; got {0!a}'.format(ns_addr,))
      
      # Normalize ns_addr argument
      self.ns_addr = (str(ip_address.ip_address_build(ns_addr[0])), int(ns_addr[1]))
      self.queries = {}
   
   def data_process(self, data, source):
      try:
         dns_frame = DNSFrame.build_from_binstream(BytesIO(data))
      except ValueError:
         self.log(30, '{0!a} got udp frame {1!a} not parsable as dns data from'
            '{2!a}. Ignoring. Parsing error was:'.format(self, data, source),
            exc_info=True)
         return
      
      if (source != self.ns_addr):
         self.log(30, '{0!a} got spurious udp frame from {1!a}; target NS is'
            'at {2!a}. Ignoring.'.format(self, source, self.ns_addr))
         return
      
      if (not (dns_frame.header.id in self.queries)):
         self.log(30, '{0!a} got spurious (unexpected id) query dns response'
            '{1!a} from {2!a}. Ignoring.'.format(self, dns_frame, source))
      
      for query in self.queries[dns_frame.header.id][:]:
         if (query.potential_response_process(dns_frame)):
            self.queries[dns_frame.header.id].remove(query)
            break
      else:
         self.log(30, '{0!a} got spurious (unexpected question section) query'
            'dns response {1!a} from {2!a}. Ignoring.'.format(self, dns_frame, source))
   
   def query_forget(self, query):
      """Forget outstanding dns query"""
      self.queries[query.id].remove(query)
   
   def id_suggestion_get(self):
      """Return suggestion for a frame id to use"""
      return random.randint(0, 2**16-1)
   
   def query_add(self, query):
      """Register new outstanding dns query and send query frame"""
      dns_frame = DNSFrame(questions=(query.question,), id=query.id)
      dns_frame_str = dns_frame.binary_repr()
      
      if (not (query.id in self.queries)):
         self.queries[query.id] = []
      
      query_list = self.queries[query.id]
      
      if (query in query_list):
         raise ValueError('query {0!a} is already registered with {1!a}.'.format(query, self))
      
      query_list.append(query)
      
      self.sock.fl.sendto(dns_frame_str, self.ns_addr)
   
   def close_process(self, fd):
      """Process close of UDP socket"""
      if not (fd == self.sock.fd):
         raise ValueError('{0!a} is not responsible for fd {1!a}'.fomat(self, fd))
      
      if (not self.cleaning_up):
         self.log(30, 'UDP socket of {0!a} is unexpectedly being closed.' % (self,))
         self.sock = None
         # Don't raise an exception here; this is most likely being called as a
         # result of another exception, which we wouldn't want to mask.
   
   def clean_up(self):
      """Shutdown instance, if still active"""
      self.cleaning_up = True
      if not (self.sock is None):
         self.sock.clean_up()
         self.sock = None
      for query_list in self.queries.values():
         for query in query_list[:]:
            query.failure_report()
            query.clean_up()
      self.queries = {}
      self.cleaning_up = False


class DNSLookupResult:
   def __init__(self, query_name, answers, additional_records):
      self.query_name = query_name
         
      self.answers = answers
      self.additional_records = additional_records
   
   def get_rr_bytypes(self, rtypes):
      return tuple([a.data_get() for a in self.answers if (a.type in rtypes)])

   def get_rr_A(self):
      return self.get_rr_bytypes((RDATA_A.type,))

   def get_rr_AAAA(self):
      return self.get_rr_bytypes((RDATA_AAAA.type,))
   
   def get_rr_ip_addresses(self):
      return self.get_rr_bytypes((RDATA_A.type, RDATA_AAAA.type))
   
   def get_rr_MX(self):
      return self.get_rr_bytypes((RDATA_MX.type,))

   def get_rr_TXT(self):
      return self.get_rr_bytypes((RDATA_TXT.type,))

   def __repr__(self):
      return '%s%s' % (self.__class__.__name__, (self.query_name, self.answers,
         self.additional_records))

   def __nonzero__(self):
      return True


class SimpleDNSQuery:
   """DNSQuery wrapper with more comfortable call syntax"""
   logger = logging.getLogger('gonium.dns_resolving.SimpleDNSQuery')
   log = logger.log
   def __init__(self, lookup_manager, result_handler, query_name, qtypes, timeout):
      query_name = DomainName(query_name)
      self.lookup_manager = lookup_manager
      self.result_handler = result_handler
      self.query_name = query_name
      self.qtypes = qtypes
      self.results = []
      self.queries = []
      self.qtype_special = False
      for qtype in self.qtypes:
         question = DNSQuestion(query_name, qtype)
         self.queries.append(DNSQuery(lookup_manager=lookup_manager,
            result_handler=self.query_result_handler,
            id=lookup_manager.id_suggestion_get(), question=question,
            timeout=timeout))
         
         if (qtype in QTYPES_SPECIAL):
            self.qtype_special = True
   
   def query_result_handler(self, query, result):
      """Process result for wrapped query"""
      self.results.append(result)
      if (len(self.results) >= len(self.qtypes)):
         self.query_results_process()
   
   def query_results_process(self):
      """Collocate and return query results"""
      valid_results = []
      valid_ars = []
      names_valid = set((self.query_name,))
      
      results = [x for x in self.results if (not (x is None))]
      
      if (len(results) == 0):
         self.result_handler(self, None)
         return
      
      for result in results:
         if (result is None):
            continue
         for answer in result.answers:
            if (not (answer.name in names_valid)):
               self.log(30, "{0!a} got bogus answer {1!a}; didn't expect this name. Ignoring.".format(self, answer)) 
               continue
            if (answer.type == RDATA_CNAME.type):
               names_valid.add(answer.rdata.domain_name)
            
            elif (not ((answer.type in self.qtypes) or self.qtype_special)):
               self.log(30, "{0!a} got bogus answer {1!a}; didn't expect this type. Ignoring.".format(self, answer))
               continue
            
            if not (answer.rdata in valid_results):
               valid_results.append(answer.rdata)
         
         for ar in result.ar:
            if not (ar in valid_ars):
               valid_ars.append(ar)
      
      res = DNSLookupResult(self.query_name, valid_results, valid_ars)
      self.result_handler(self, res)
      self.result_handler = None
      self.lookup_manager = None
      self.queries = ()
   
   def clean_up(self):
      """Cancel request, if still pending"""
      for query in self.queries:
         query.clean_up
      self.queries = ()
      self.result_handler = None
      self.lookup_manager = None

# ----------------------------------------------------------------------------- selftest code

def _module_selftest_local():
   question_1 = DNSQuestion(DomainName(b'www.example.net'),RDATA_A.type,1)
   question_2 = DNSQuestion(DomainName(b'example.net'),RDATA_AAAA.type,1)
   
   f1_0 = DNSFrame(questions=[question_1, question_2],id=1234)
   r1_0 = f1_0.binary_repr()
   f1_1 = DNSFrame.build_from_binstream(BytesIO(r1_0))
   r1_1 = f1_1.binary_repr()
   if (r1_0 != r1_1):
      raise StandardError('Idempotency test 01 failed.')
   
   answer_1 = ResourceRecord(name=DomainName(b'www.example.net'), rtype=RDATA_AAAA.type, 
      rdata=RDATA_AAAA(ip_address.ip_address_build(b'::1')), ttl=0)
   answer_2 = ResourceRecord(name=DomainName(b'www.example.net'), rtype=RDATA_A.type, 
      rdata=RDATA_A(ip_address.ip_address_build(b'127.0.0.1')), ttl=0)
   
   f2_0 = DNSFrame(questions=[question_1, question_2], answers=[answer_1, answer_2], ar=[answer_1, answer_2], id=1235)
   r2_0 = f2_0.binary_repr()
   f2_1 = DNSFrame.build_from_binstream(BytesIO(r2_0))
   r2_1 = f2_1.binary_repr()

   if (r2_0 != r2_1):
      raise StandardError('Idempotency test 02 failed.')
   return True


def _module_selftest_network(ns_addr, af=socket.AF_INET):
   s = socket.socket(af, socket.SOCK_DGRAM)
   query_data = DNSFrame(questions=[DNSQuestion(DomainName(b'www.example.net'), QTYPE_ALL)], id=1236).binary_repr()
   
   s.sendto(query_data, ns_addr)
   print('Sending query {0!a} to {1!a}...'.format(query_data, ns_addr,))
   (in_data, in_addr) = s.recvfrom(512)
   print('Got reply {0!a} from {1!a}.'.format(in_data, in_addr))
   
   response_data = DNSFrame.build_from_binstream(BytesIO(in_data))
   print('Data: {0!a}'.format(response_data))
   return True


class _StatefulLookupTester_1:
   def __init__(self, ed, ns_addr, question):
      self.ed = ed
      self.la = DNSLookupManager(self.ed, ns_addr)
      self.query = DNSQuery(self.la, self.result_handler, id=42, question=question, timeout=20)
      self.ed.event_loop()
   
   def result_handler(self, dns_query, dns_data):
      print('Got response: {0!a}'.format(dns_data,))
      self.ed.shutdown()

class _StatefulLookupTester_2:
   def __init__(self, ed, ns_addr, questionstring, qtypes):
      self.ed = ed
      self.la = DNSLookupManager(self.ed, ns_addr)
      self.query = SimpleDNSQuery(self.la, self.result_handler, questionstring, qtypes, timeout=20)
      self.ed.event_loop()
   
   def result_handler(self, dns_query, dns_data):
      print('Got response: {0!a}'.format(dns_data,))
      print('Ipv4/IPv6 addresses: {0!a}'.format(dns_data.get_rr_ip_addresses(),))
      self.ed.shutdown()


def _module_selftest_stateful_network(ns_addr):
   from .fdm import ED_get
   ED = ED_get()
   q1 = _StatefulLookupTester_1(ED(), ns_addr, DNSQuestion(DomainName(b'www.example.net'),QTYPE_ALL))
   q2 = _StatefulLookupTester_2(ED(), ns_addr, b'sixxs.net', qtypes=(RDATA_A.type, RDATA_AAAA.type))


def _selftest():
   from ._debugging import streamlogger_setup; streamlogger_setup()
   
   print('Testing stateless components, locally only...')
   _module_selftest_local()
   print('...test done.\n')
   
   print('Testing stateless components, using network connections...')
   import sys
   ns_ipaddr = sys.argv[1]
   ns_port = int(sys.argv[2])
   _module_selftest_network(ns_addr=(ns_ipaddr, ns_port))
   print('...test done.\n')
   
   print('Testing stateful components, using network connections; if this hangs, you probably have a problem:')
   _module_selftest_stateful_network((ns_ipaddr, ns_port))
   print('...test done.\n')
   
   print('Self-test passed.')

if (__name__ == '__main__'):
   _selftest()
