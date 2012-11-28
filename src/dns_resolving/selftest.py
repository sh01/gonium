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
from .. import ip_address
from ..ip_address import IPAddressV4, IPAddressV6, ip_address_build
from ..fdm.packet import AsyncPacketSock

from .stub import *

# ----------------------------------------------------------------------------- selftest code
def _module_selftest_local():
   question_1 = DNSQuestion(DomainName(b'www.example.net'),QTYPE_A,1)
   question_2 = DNSQuestion(DomainName(b'example.net'),QTYPE_AAAA,1)
   
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


def _module_selftest_network(ns_addr):
   af = ip_address_build(ns_addr[0]).AF
   s = socket.socket(af, socket.SOCK_DGRAM)
   query_data = DNSFrame(questions=[DNSQuestion(DomainName(b'www.example.net'), QTYPE_ALL)], id=1236).binary_repr()

   s.sendto(query_data, ns_addr)
   print('Sending query {!a} to {!a}...'.format(query_data, ns_addr,))
   (in_data, in_addr) = s.recvfrom(512)
   print('Got reply {!a} from {!a}.'.format(in_data, in_addr))
   
   response_data = DNSFrame.build_from_binstream(BytesIO(in_data))
   print('Data: {!a}'.format(response_data))
   return True


class _StatefulLookupTester_1:
   def __init__(self, ed, blm, question):
      self.ed = ed
      self.la = blm(self.ed)
      self.query = DNSQuery(self.la, self.result_handler, id=42, question=question, timeout=20)
      self.ed.event_loop()
   
   def result_handler(self, dns_query, dns_data):
      print('Got response: {!a}'.format(dns_data,))
      self.ed.shutdown()

class _StatefulLookupTester_2:
   def __init__(self, ed, blm, questionstring, qtypes):
      self.ed = ed
      self.la = blm(self.ed)
      self.query = SimpleDNSQuery(self.la, self.result_handler, questionstring, qtypes, timeout=20)
      self.ed.event_loop()
   
   def result_handler(self, dns_query, dns_data):
      print('Got response: {!a}'.format(dns_data,))
      print('Ipv4/IPv6 addresses: {!a}'.format(dns_data.get_rr_ip_addresses(),))
      self.ed.shutdown()


def _module_selftest_stateful_network(blm):
   from ..fdm import ED_get
   ED = ED_get()
   q1 = _StatefulLookupTester_1(ED(), blm, DNSQuestion(DomainName(b'www.example.net'),QTYPE_ALL))
   q2 = _StatefulLookupTester_2(ED(), blm, b'sixxs.net', qtypes=(QTYPE_A, QTYPE_AAAA))


def _selftest():
   from .._debugging import streamlogger_setup; streamlogger_setup()
   import optparse
   
   op = optparse.OptionParser()
   op.add_option('--ns_ip', default=None, help='Set nameserver IP to this value instead of parsing from config.')
   op.add_option('--ns_port', default=53, help='Nameserver port to use in conjunction with ns_ip.')
   op.add_option('--config_fn', default='/etc/resolv.conf', help='Path of resolv.conf to use.')
   
   (opts, args) = op.parse_args()
   if (opts.ns_ip):
      ns_addr = (opts.ns_ip, int(opts.ns_port))
      def blm(*args):
         return DNSLookupManager(*args, ns_addr=ns_addr)
   else:
      rc = ResolverConfig.build_from_file(opts.config_fn)
      ns_addr = rc.get_addr()
      blm = rc.build_lookup_manager
   
   print('==== Testing stateless components, locally only')
   _module_selftest_local()
   print('...test done.\n')
   
   print('==== Testing stateless components, using network connections')
   _module_selftest_network(ns_addr)
   print('...test done.\n')
   
   print('==== Testing stateful components, using network connections (if this hangs, you probably have a problem)')
   _module_selftest_stateful_network(blm)
   print('...test done.\n')
   
   print('Self-test passed.')

if (__name__ == '__main__'):
   _selftest()
