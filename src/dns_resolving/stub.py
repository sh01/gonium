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

from collections import deque
import logging
import struct
import socket
from io import BytesIO
import random

from .base import *
from .. import ip_address
from ..ip_address import ip_address_build
from ..fdm.packet import AsyncPacketSock
from ..fdm.stream import AsyncDataStream

# ----------------------------------------------------------------------------- question / RR sections

class ValueVerifier:
   NAME_MIN = 0
   NAME_MAX = 255
   TYPE_MIN = 1
   TYPE_MAX = 255
   @classmethod
   def name_validate(cls, name):
      if not (cls.NAME_MIN <= len(name) <= cls.NAME_MAX):
         raise ValueError('NAME {!a} is invalid'.format(name,))
   
   @classmethod
   def type_validate(cls, rtype):
      if not (cls.TYPE_MIN <= int(rtype) <= cls.TYPE_MAX):
         raise ValueError('TYPE {!a} is invalid'.format(rtype,))


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

   def __eq__(self, other):
      return (self.binary_repr() == other.binary_repr())
   def __ne__(self, other):
      return not (self == other)
   def __hash__(self):
      return hash(self.binary_repr())


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
         raise ValueError('Expected value to lie between {} and {}; got {!a} instead.'.format(limit_min, limit_max, val))

   @classmethod
   def build_from_binstream(cls, binstream):
      s = binstream.read(12)
      if (len(s) < 12):
         raise ValueError('Insufficient data in stream')
      
      return cls.build_from_binstring(s)
   
   @classmethod
   def build_from_binstring(cls, binstring):
      if (len(binstring) != 12):
         raise ValueError('Binstring {!a} has invalid length'.format(binstring,))
      
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
         raise ValueError('Value {!a} for argument question is invalid'.format(question,))
      
      self.id = id
      self.result_handler = result_handler
      self.question = question
      self.la = lookup_manager
      self.la.query_add(self)
      if not (timeout is None):
         self.tt = self.la.event_dispatcher.set_timer(timeout, self.timeout_process, parent=self)
   
   def __eq__(self, other):
      return ((self.id == other.id) and (self.question == other.question))
   def __ne__(self, other):
      return not (self == other)
   def __hash__(self):
      return hash((self.id, self.question))

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
   
   def is_response(self, response):
      """Check a response for whether it answers our query. Do not process it further either way."""
      return (tuple(response.questions) == (self.question,))
   
   def potential_response_process(self, response):
      """Check a response for whether it answers our query, and if so process it.
      
      Returns whether the response was accepted."""
      if (not self.is_response(response)):
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
   
   def get_dns_frame(self):
      return DNSFrame(questions=(self.question,), id=self.id)

class ResolverConfig:
   DEFAULT_FN = '/etc/resolv.conf'
   PORT = 53
   def __init__(self, nameservers):
      self.ns = nameservers

   @classmethod
   def build_from_file(cls, fn=None):
      if (fn is None):
         fn = cls.DEFAULT_FN

      nameservers = []
      try:
         f = open(fn, 'r')
      except IOError:
         pass
      else:
         for line in f:
            words = line.split()
            if (not words):
               continue
            if (words[0].startswith('#')):
               continue
            if (words[0] == 'nameserver'):
               if (len(words) > 1):
                  try:
                     ip = ip_address_build(words[1])
                  except ValueError:
                     continue
                  nameservers.append(ip)
               continue
         f.close()

      if (not nameservers):
         nameservers = [ip_address_build(s) for s in ('127.0.0.1','::1')]

      return cls(nameservers)

   def get_addr(self):
      return (str(self.ns[0]), self.PORT)
      
   def build_lookup_manager(self, ed):
      return DNSLookupManager(ed, ns_addr=self.get_addr())


class DNSTCPStream(AsyncDataStream):
   def __init__(self, *args, **kwargs):
      super().__init__(*args, **kwargs)
      self.size = 2
   
   def process_input(self, data):
      bytes_used = 0
      bytes_left = len(data)
      msgs = []
      while (bytes_left > 2):
        (l,) = struct.unpack('>H', data[bytes_used:bytes_used+2])
        wb = l + 2
        if (wb > bytes_left):
          self.size = wb
          break
        msgs.append(data[bytes_used+2:bytes_used+wb])
        bytes_used += wb
        bytes_left -= wb
      else:
         self.size = 2
      self.discard_inbuf_data(bytes_used)
      
      if (msgs):
         self.process_msgs(msgs)

   def send_query(self, query):
      frame_data = query.get_dns_frame().binary_repr()
      try:
        header = struct.pack('>H', (len(frame_data)))
      except struct.error as exc:
        raise ValueError('Too much data.') from struct.error
      self.send_bytes((header, frame_data))


class DNSLookupManager:
   logger = logging.getLogger('gonium.dns_resolving.DNSLookupManager')
   log = logger.log
   def __init__(self, event_dispatcher, ns_addr, addr_family=None):
      self.event_dispatcher = event_dispatcher
      self.cleaning_up = False
      if (not (len(ns_addr) == 2)):
         raise ValueError('Argument ns_addr should have two elements; got {!a}'.format(ns_addr,))
      
      ip_addr = ip_address.ip_address_build(ns_addr[0])
      if (addr_family is None):
         addr_family = ip_addr.AF
      
      sock = socket.socket(addr_family, socket.SOCK_DGRAM)
      self.sock_udp = AsyncPacketSock(self.event_dispatcher, sock)
      self.sock_udp.process_input = self.data_process
      self.sock_udp.process_close = self.close_process
      self.sock_tcp = None
      self.sock_tcp_connected = False
      self._qq_tcp = deque()
      
      # Normalize ns_addr argument
      self.ns_addr = (str(ip_addr), int(ns_addr[1]))
      self.queries = {}
   
   def _have_tcp_connection(self):
      s = self.sock_tcp
      return (s and (s.state == s.CS_UP))
   
   def _send_tcp(self, query):
      if (self._have_tcp_connection()):
        try:
          self.sock_tcp.send_query(query)
        except ValueError:
          self.log(40, '{!a} unable to send query over TCP:'.format(self), exc_info=True)
          self.event_dispatcher.set_timer(0, query.timeout_process, parent=self, interval_relative=False)
      else:
        if (self.sock_tcp is None):
          self._make_tcp_sock()
        self._qq_tcp.append(query)
        return 

   def _make_tcp_sock(self):
      self.sock_tcp = s = DNSTCPStream(run_start=False)
      s.process_close = self._process_tcp_close
      s.connect_async_sock(self.event_dispatcher, ip_address_build(self.ns_addr[0]), self.ns_addr[1], connect_callback=self._process_tcp_connect)
      s.process_msgs = self._process_tcp_msgs
   
   def _process_tcp_connect(self, conn):
      for query in self._qq_tcp:
        self.sock_tcp.send_query(query)
      self._qq_tcp.clear()

   def _process_tcp_close(self):
      self.sock_tcp = None
   
   def _process_tcp_msgs(self, msgs):
      for msg in msgs:
        self.data_process(msg, self.ns_addr, tcp=True)
   
   def data_process(self, data, source, tcp=False):
      try:
         dns_frame = DNSFrame.build_from_binstream(BytesIO(data))
      except ValueError:
         self.log(30, '{!a} got frame {!a} not parsable as dns data from {!a}. Ignoring. Parsing error was:'.format(self, bytes(data), source), exc_info=True)
         return
      
      if (source != self.ns_addr):
         self.log(30, '{!a} got spurious udp frame from {!a}; target NS is at {!a}. Ignoring.'.format(self, source, self.ns_addr))
         return
      
      if (not (dns_frame.header.id in self.queries)):
         self.log(30, '{!a} got spurious (unexpected id) query dns response {!a} from {!a}. Ignoring.'.format(self, dns_frame, source))
         return
      

      def log_spurious():
         self.log(30, '{!a} got spurious (unexpected question section) query dns response {!a} from {!a}. Ignoring.'.format(self, dns_frame, source))
      if (dns_frame.header.truncation):
         if (tcp):
            self.log(30, '{!a} got truncated dns response {!a} over TCP from {!a}. Ignoring.'.format(self, dns_frame, source))
            return
         self.log(25, '{!a} got truncated dns response {!a} from {!a}. Retrying over TCP.'.format(self, dns_frame, source))

         for query in self.queries[dns_frame.header.id]:
            if (query.is_response(dns_frame)):
               self._send_tcp(query)
               break
         else:
            log_spurious()
         return
      
      for query in self.queries[dns_frame.header.id][:]:
         if (query.potential_response_process(dns_frame)):
            self.queries[dns_frame.header.id].remove(query)
            break
      else:
         log_spurious()
   
   def query_forget(self, query):
      """Forget outstanding dns query"""
      self.queries[query.id].remove(query)
      try:
         self._qq_tcp.remove(query)
      except ValueError:
         pass
   
   def id_suggestion_get(self):
      """Return suggestion for a frame id to use"""
      while True:
        rv = random.randint(0, 2**16-1)
        if not (rv in self.queries):
          return rv
   
   def query_add(self, query):
      """Register new outstanding dns query and send query frame"""
      dns_frame_str = query.get_dns_frame().binary_repr()
      
      if (not (query.id in self.queries)):
         self.queries[query.id] = []
      
      query_list = self.queries[query.id]
      
      if (query in query_list):
         raise ValueError('query {!a} is already registered with {!a}.'.format(query, self))
      
      query_list.append(query)
      
      self.sock_udp.fl.sendto(dns_frame_str, self.ns_addr)
   
   def close_process(self):
      """Process close of UDP socket"""
      #if not (fd == self.sock_udp.fd):
      #   raise ValueError('{!a} is not responsible for fd {!a}'.fomat(self, fd))
      
      if (not self.cleaning_up):
         self.log(30, 'UDP socket of {!a} is unexpectedly being closed.'.format(self))
         self.sock_udp = None
         # Don't raise an exception here; this is most likely being called as a
         # result of another exception, which we wouldn't want to mask.
   
   def clean_up(self):
      """Shutdown instance, if still active"""
      self.cleaning_up = True
      if not (self.sock_udp is None):
         self.sock_udp.clean_up()
         self.sock_udp = None
      for query_list in self.queries.values():
         for query in query_list[:]:
            query.failure_report()
            query.clean_up()
      self.queries.clear()
      self._qq_tcp.clear()
      self.cleaning_up = False

   def build_simple_query(self, *args, **kwargs):
      return SimpleDNSQuery(self, *args, **kwargs)


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
      if (isinstance(query_name, str)):
         query_name = query_name.encode('ascii')

      if (query_name.endswith(b'.') and not query_name.endswith(b'..')):
         query_name = query_name[:-1]
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
               self.log(30, "{!a} got bogus answer {!a}; didn't expect this name. Ignoring.".format(self, answer)) 
               continue
            if (answer.type == RDATA_CNAME.type):
               names_valid.add(answer.rdata.domain_name)
            
            elif (not ((answer.type in self.qtypes) or self.qtype_special)):
               self.log(30, "{!a} got bogus answer {!a}; didn't expect this type. Ignoring.".format(self, answer))
               continue
            
            if not (answer.rdata in valid_results):
               valid_results.append(answer.rdata)
         
         for ar in result.ar:
            if not (ar in valid_ars):
               valid_ars.append(ar)
      
      res = DNSLookupResult(self.query_name, valid_results, valid_ars)
      try:
        self.result_handler(self, res)
      except BaseException as exc:
         self.log(40, 'Error on DNS lookup result processing for {!a}:'.format(res), exc_info=True)

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

