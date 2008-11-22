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

# Iptables is a lot of things, but easy-linkable-to-python-C-modules isn't one
# of them. In addition, libiptc isn't built as a shared library by default,
# and is in any event not distributed by debian (anymore).
# OTOH, changes to the linux kernel-userspace ABI are supposed to be
# backwards-compatible in almost all cases. This suggests that independent
# implementations of libiptc functionality should stay compatible over many
# future Linux versions.
# Moreover, in a HLL such an implementation is possible with a reasonable
# amount of effort.
# This module attempts to be such an implementation.
#
# It should now be stable for reading xtables data, though note that it can't
# parse match or target *data* (match/target names are available, and so is
# the raw data associated with them, there just isn't any code for further
# parsing the latter).

import ctypes
import socket
import struct
import sys
import time
from socket import SOL_IP
SOL_IPV6 = 41

from gonium.ip_address import IPAddressV4, IPAddressV6
from gonium.event_multiplexing import EventMultiplexer
from gonium.fd_management import Timer

NF_IP_NUMHOOKS = 5
NF_IP6_NUMHOOKS = 5
NF_ARP_NUMHOOKS = 3
# Taken from /usr/include/linux/netfilter/x_tables.h
XT_TABLE_MAXNAMELEN = 32
XT_FUNCTION_MAXNAMELEN = 30
# Taken from /usr/include/linux/if.h
IFNAMSIZ = 16
# Taken from /usr/include/linux/netfilter_arp/arp_tables.h
ARPT_DEV_ADDR_LEN_MAX = 16

# Python's socket.socket.getsockopt() wrapper doesn't allow us to specify the
# initial contents of the memory pointed to by optval. This functionality is
# required to interface with netfilter. Hence, we use getsockopt through
# ctypes.
libc = ctypes.CDLL('libc.so.6')
libc.getsockopt.argtypes = (ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_void_p, type(ctypes.pointer(ctypes.c_int())))


class XTProtocolError(ValueError):
   pass


def cstring_trim(s):
   """Trim a string to just before the first '\x00' element"""
   i = s.find('\x00')
   if (i == -1):
      return s
   return s[:i]


class XTGetInfo:
   __fields__ = ('name', 'valid_hooks', 'hook_entry', 'underflow', 'num_entries', 'size')
   def __init__(self, name, valid_hooks, hook_entry, underflow, num_entries, size):
      for field in self.__fields__:
         setattr(self,field,locals()[field])
   
   def get_valid_hook_entries(self):
      """Return a list with those of our hook_entry points that are valid"""
      i = 1
      rv = []
      for j in range(len(self.hook_entry)):
         if (i & self.valid_hooks):
            rv.append((self.hook_entry[j],j))
         i <<= 1
      return rv
   
   @classmethod
   def build_from_bindata(cls, data):
      values = struct.unpack(cls.fmt, data)
      cname = ctypes.create_string_buffer(XT_TABLE_MAXNAMELEN)
      cname.raw = values[0]
      return cls(cname.value, 
         values[1],
         values[2:2+cls.NUMHOOKS],
         values[2+cls.NUMHOOKS:2+cls.NUMHOOKS+cls.NUMHOOKS],
         values[2+cls.NUMHOOKS+cls.NUMHOOKS],
         values[2+cls.NUMHOOKS+cls.NUMHOOKS+1]
      )
   
   def __repr__(self):
      return '%s%s' % (self.__class__.__name__, tuple([getattr(self,name) for name in self.__fields__]))

_xtgetinfo_fmt = '%ssI%sI%sIII'
class XTGetInfo_IP(XTGetInfo):
   NUMHOOKS = NF_IP_NUMHOOKS
   fmt = _xtgetinfo_fmt % (XT_TABLE_MAXNAMELEN, NUMHOOKS, NUMHOOKS)
   fmt_size = struct.calcsize(fmt)

class XTGetInfo_IP6(XTGetInfo):
   NUMHOOKS = NF_IP6_NUMHOOKS
   fmt = _xtgetinfo_fmt % (XT_TABLE_MAXNAMELEN, NUMHOOKS, NUMHOOKS)
   fmt_size = struct.calcsize(fmt)

class XTGetInfo_ARP(XTGetInfo):
   NUMHOOKS = NF_ARP_NUMHOOKS
   fmt = _xtgetinfo_fmt % (XT_TABLE_MAXNAMELEN, NUMHOOKS, NUMHOOKS)
   fmt_size = struct.calcsize(fmt)

class BinPacker(dict):
   """Efficient field access for packed binary strings; currently read-only"""
   Q_mod = 2**64
   def __init__(self):
      self.fmt = '@'
   
   def field_add(self, name, fmt, prefix='', ppf=None):
      if (name in self):
         raise ValueError('I already contain %r.' % (name,))
      size = struct.calcsize(fmt)
      if (fmt and (fmt[0] in '@=<>!')):
         raise ValueError('Invalid first char in value %r for argument fmt; use prefix for that.' % (fmt,))
      
      # This is somewhat hackish: Assuming that padding is always inserted in
      # front of struct members allows us to determine offsets for individual
      # fields.
      offset = struct.calcsize(self.fmt + fmt)
      size = struct.calcsize(fmt)
      
      if not (name is None):
         self[name] = (offset-size, prefix + fmt, size, ppf)
      self.fmt += fmt
   
   def field_get(self, name, data, offset2=0, size2=0):
      (offset, fmt, size, ppf) = self[name]
      offset += offset2
      size += size2
      srv = struct.unpack(fmt, data[offset:offset+size])
      if (ppf):
         rv = ppf(*srv)
      else:
         (rv,) = srv
      return rv
   
   def size_get(self):
      return struct.calcsize(self.fmt)
   
   # The struct layouts implicit in the following code are taken from the
   # various linux/neftiler*/*_tables.h include files.
   def xte_fields_add1(self):
      ifnamsiz = str(IFNAMSIZ) + 's'
      self.field_add('iface_in', ifnamsiz, '', cstring_trim)
      self.field_add('iface_out',  ifnamsiz, '', cstring_trim)
      self.field_add('iface_in_mask', ifnamsiz, '', cstring_trim)
      self.field_add('iface_out_mask', ifnamsiz, '', cstring_trim)
      
   def xte_fields_add2(self):
      self.field_add('offset_target', 'H', '')
      self.field_add('offset_next', 'H', '')
      self.field_add('comefrom', 'I', '>')
      self.field_add('counter_packets', 'Q', '')
      self.field_add('counter_bytes', 'Q', '')
      self.field_add(None, '0Q') #outer struct alignment

   @classmethod
   def build_xte_ip(cls):
      self = cls()
      self.field_add('src','I', '>', IPAddressV4)
      self.field_add('dst','I', '>', IPAddressV4)
      self.field_add('src_mask', 'I', '>', IPAddressV4)
      self.field_add('dst_mask', 'I', '>', IPAddressV4)
      self.xte_fields_add1()
      self.field_add('proto', 'H', '>')
      self.field_add('flags', 'B', '')
      self.field_add('invflags', 'B', '')
      self.field_add(None, '0I')
      self.field_add('nfcache', 'I', '>')
      self.xte_fields_add2()
      return self
   
   @classmethod
   def build_xte_ip6(cls):
      self = cls()
      def ipv6_make(q1, q2):
         return IPAddressV6(q1*cls.Q_mod + q2)
      
      self.field_add('src', 'QQ', '>', ipv6_make)
      self.field_add('dst', 'QQ', '>', ipv6_make)
      self.field_add('src_mask', 'QQ', '>', ipv6_make)
      self.field_add('dst_mask', 'QQ', '>', ipv6_make)
      self.xte_fields_add1()
      self.field_add('proto', 'H', '>')
      self.field_add('tos', 'B', '')
      self.field_add('flags', 'B', '')
      self.field_add('invflags', 'B', '')
      self.field_add(None, '0I')
      self.field_add('nfcache', 'I', '>')
      self.xte_fields_add2()
      return self
   
   @classmethod
   def build_xte_arp(cls):
      fmt_adalm = '%ds' % ARPT_DEV_ADDR_LEN_MAX
      self = cls()
      self.field_add('src','I', '>', IPAddressV4)
      self.field_add('dst','I', '>', IPAddressV4)
      self.field_add('src_mask', 'I', '>', IPAddressV4)
      self.field_add('dst_mask', 'I', '>', IPAddressV4)
      self.field_add('ah_len', 'B')
      self.field_add('ah_mask', 'B')
      self.field_add('src_devaddr', fmt_adalm)
      self.field_add('src_devaddr_mask', fmt_adalm)
      self.field_add('dst_devaddr', fmt_adalm)
      self.field_add('dst_devadd_mask', fmt_adalm)
      self.field_add('arpop', 'H', '>')
      self.field_add('arpop_mask', 'H', '>')
      self.field_add('arhrd', 'H', '>')
      self.field_add('arhrd_mask', 'H', '>')
      self.field_add('arpro', 'H', '>')
      self.field_add('arpro_mask', 'H', '>')
      self.xte_fields_add1()
      self.field_add('flags', 'B', '')
      self.field_add('invflags', 'H', '')
      self.field_add(None, '0I')
      self.xte_fields_add2()
      return self
   
   @classmethod
   def build_xte_target(cls):
      self = cls()
      self.field_add('target_size', 'H')
      self.field_add('target_name', '%ss' % (XT_FUNCTION_MAXNAMELEN-1,), '', cstring_trim)
      self.field_add('target_rev', 'B')
      self.field_add(None, '0H')
      return self
   
   @classmethod
   def build_xte_match(cls):
      self = cls()
      self.field_add('size', 'H')
      self.field_add('name', '%ss' % (XT_FUNCTION_MAXNAMELEN-1,), '', cstring_trim)
      self.field_add('rev', 'B')
      self.field_add(None, '0H')
      return self


class XTEntry_Match:
   bp = BinPacker.build_xte_match()
   __fields__ = ('xtentry', 'offset', 'size')
   def __init__(self, xtentry, offset):
      self.xtentry = xtentry
      self.offset = offset
   
   def __getattr__(self, name):
      try:
         return self.bp.field_get(name, self.xtentry.data, self.offset)
      except KeyError:
         if (name == 'data'):
            try:
               return self.data_get()
            except:
               pass
      raise AttributeError('%r doesn\'t have attribute %r' % (self, name))
   
   def data_get(self):
      return self.xtentry.data[self.offset+self.bp.size_get():self.offset+self.size]
   
   def __repr__(self):
      return '<%s name %s data %r>' % (self.__class__.__name__, self.name, self.data)


class XTEntry_Base:
   bp_target = BinPacker.build_xte_target()
   bp = None #Should be overridden by subclass
   
   # taken from /usr/include/linux/netfilter.h
   NF_DROP = 0
   NF_ACCEPT = 1
   NF_STOLEN = 2
   NF_QUEUE = 3
   NF_REPEAT = 4
   NF_STOP = 5

   XT_VERDICTS = {}
   for name in ('DROP', 'ACCEPT', 'STOLEN', 'QUEUE', 'REPEAT', 'STOP'):
      XT_VERDICTS[-1*vars()['NF_' + name]-1] = name
   
   def __init__(self, data):
      self.data = data
      self.offset_target = self.bp.field_get('offset_target',self.data)
      self.bin_unpackers = {'target_data':self.target_data_get}
      self.hook_entry = False # to be fixed up afterwards
      self.chain = None       # to be fixed up afterwards
      self.target = self._target_get()
      matches = []
      offset = self.bp.size_get()
      while (offset < self.offset_target):
         match = XTEntry_Match(self, offset)
         matches.append(match)
         offset += match.size
      
      self.matches = tuple(matches)
      
      if (offset != self.offset_target):
         raise XTProtocolError("Sanity check failed: match parsing resulted in next offset %d, start of target section is at offset %d." % (offset, self.offset_target))
      
      if (self.offset_target+self.target_size != len(self.data)):
         raise XTProtocolError("Sanity check failed: entry length is %r, end of target section is at offset %r." % (len(self.data), self.offset_target+self.target_size))
   
   def is_chainstart(self):
      return bool(self.hook_entry or (cstring_trim(self.target_name) == 'ERROR'))
   
   def get_chain_name(self):
      if not self.is_chainstart():
         return self.chain
      if (self.target_name == 'ERROR'):
         return cstring_trim(self.target_data)
      return self.hook_names[self.hook_entry - 1]
   
   def _verdict_get(self):
      """Return verdict data"""
      (verdict,) = struct.unpack('i0Q', self.target_data)
      return verdict
   
   def _target_get(self):
      """Try to compute target; doesn't deal with jumps"""
      if (self.target_name):
         return self.target_name
      
      verdict = self._verdict_get()
      if (verdict < 0):
         return self.XT_VERDICTS[verdict]
   
   def target_data_get(self):
      return self.data[self.offset_target+struct.calcsize(self.bp_target.fmt):]
   
   def __getattr__(self, name):
      for (bp, offset) in ((self.bp,0), (self.bp_target,self.offset_target)):
         try:
            return bp.field_get(name, self.data, offset)
         except KeyError:
            continue
      try:
         return self.bin_unpackers[name]()
      except KeyError:
         pass
      raise AttributeError("%r doesn't have attribute %r." % (type(self), name))
   
   def __repr__(self):
      target = self.target
      if (hasattr(target, 'get_chain_name')):
         try:
            target = target.get_chain_name()
         except:
            pass
      return '<%s instance at %s: %d %d %s .. .. %r %r %s/%s %s/%s m %s>' % (
         self.__class__.__name__, id(self),
         self.counter_packets, self.counter_bytes, target,
         self.iface_in, self.iface_out, self.src, self.src_mask, self.dst,
         self.dst_mask, self.matches)
   
   def fields_get(self):
      rv = {'hook_entry':self.hook_entry}
      for bp in (self.bp, self.bp_target, self.bin_unpackers):
         for name in bp:
            rv[name] = getattr(self, name)
      
      return rv

class XTEntry_IP(XTEntry_Base):
   bp = BinPacker.build_xte_ip()
   # from /usr/include/linux/netfilter_ipv4.h
   hook_names = (
      'PREROUTING', #0
      'INPUT',      #1
      'FORWARD',    #2
      'OUTPUT',     #3
      'POSTROUTING' #4
   )
   
class XTEntry_IP6(XTEntry_Base):
   bp = BinPacker.build_xte_ip6()
   # from /usr/include/linux/netfilter_ipv6.h
   hook_names = (
      'PREROUTING', #0
      'INPUT',      #1
      'FORWARD',    #2
      'OUTPUT',     #3
      'POSTROUTING' #4
   )


class XTEntry_ARP(XTEntry_Base):
   bp = BinPacker.build_xte_arp()
   # from /usr/include/linux/netfilter_arp.h
   hook_names = (
      'INPUT',   #0
      'OUTPUT',  #1
      'FORWARD', #2
   )

class XTGEContainer:
   def __init__(self, xtge, ts_low, ts_high):
      self.xtge = xtge
      self.ts_low = ts_low
      self.ts_high = ts_high


class XTGetEntries_Base:
   __fields__ = ('name', 'entries')
   fmts = '%ssI0Q' % (XT_TABLE_MAXNAMELEN,)
   fmts_size = struct.calcsize(fmts)
   
   def __init__(self, name, entries):
      for field in self.__fields__:
         setattr(self,field,locals()[field])
   
   def get_chains(self):
      """Return rules sorted into chains"""
      rv = {}
      l = None
      done = False
      for entry in self.entries[:-1]:
         if not (entry.is_chainstart()):
            l.append(entry)
            entry.chain = chain_name
            continue

         chain_name = entry.get_chain_name()
         if (chain_name in rv):
            raise XTProtocolError('Duplicated chain name %r.' % (chain_name,))
         l = rv[chain_name] = []
         if (entry.target_name == 'ERROR'):
            # user-defined chain
            continue
         
         # built-in chain, therefore this is also a regular entry
         l.append(entry)
         entry.chain = chain_name
      
      return rv

   @classmethod
   def build_from_bindata(cls, xtgi, data):
      values = struct.unpack(cls.fmts, data[:cls.fmts_size])
      cname = ctypes.create_string_buffer(XT_TABLE_MAXNAMELEN)
      cname.raw = values[0]
      
      base_offset = cls.fmts_size
      offset = base_offset
      size = len(data)
      entries = []
      entries_by_offset = {}
      while (offset < size):
         offset_next = cls.xt_entry.bp.field_get('offset_next', data, offset)
         
         entry = cls.xt_entry(data[offset:offset+offset_next])
         entries.append(entry)
         entries_by_offset[offset-base_offset] = entry
         
         if (offset_next < 1):
            raise ValueError('Bogus value %r for offset_next' % (offset_next,))
         rout = repr(entries[-1].iface_in)
         offset += offset_next
         
      if (offset != size):
         raise ValueError('Final offset %r, size %r; expected equality.' % (offset, size))
      
      try:
         # Identify hook targets
         for (offset, hep_index) in xtgi.get_valid_hook_entries():
            entries_by_offset[offset].hook_entry = hep_index + 1
         
         # 2nd pass over entries to link jump targets
         for entry in entries:
            if not (entry.target is None):
               continue
            entry.target = entries_by_offset[entry._verdict_get()]
      
      except KeyError, exc_orig:
         exc_new = XTProtocolError('Entry-list postprocessing failed. Original error: %s' % (sys.exc_info(),))
         exc_new.exc_orig = exc_orig
         raise exc_new
      
      return cls(cname.value, entries)


class XTGetEntries_IP(XTGetEntries_Base):
   xt_entry = XTEntry_IP

class XTGetEntries_IP6(XTGetEntries_Base):
   xt_entry = XTEntry_IP6

class XTGetEntries_ARP(XTGetEntries_Base):
   xt_entry = XTEntry_ARP


class XTables:
   def __init__(self):
      self.sock = socket.socket(self.af, socket.SOCK_DGRAM)

   def getsockopt(self, optname, optval, optlen):
      """Getsockopt wrapper with initial-optname support"""
      buf = ctypes.create_string_buffer(optval, optlen)
      bufsize = ctypes.c_int(len(buf))
      l = libc.getsockopt(self.sock.fileno(), self.sol, optname, buf, ctypes.pointer(bufsize))
      if (l < 0):
         raise ValueError('getsockopt()-call failed')
      return buf.raw
   
   def get_info(self, table):
      """Retrieve xt_getinfo data and deserialize"""
      data = self.getsockopt(self.SO_GET_INFO, table, self.xtgi.fmt_size)
      return self.xtgi.build_from_bindata(data)
   
   def get_entries(self, table, xtgi):
      size = xtgi.size
      ts_low = time.time()
      data = self.getsockopt(self.SO_GET_ENTRIES, struct.pack(self.xtge.fmts, table, size), self.xtge.fmts_size + size)
      ts_high = time.time()
      return XTGEContainer(self.xtge.build_from_bindata(xtgi, data), ts_low, ts_high)
   
   def table_read(self, table):
      xtgi = self.get_info(table)
      xtgec = self.get_entries(table, xtgi)
      return xtgec
   
   def close(self):
      self.sock.close()


class XTablesIP(XTables):
   af = socket.AF_INET
   xtge = XTGetEntries_IP
   xtgi = XTGetInfo_IP
   sol = SOL_IP
   # Taken from /usr/include/linux/netfilter_ipv4/ip_tables.h
   BASE_CTL = 64
   SO_GET_INFO = BASE_CTL
   SO_GET_ENTRIES = BASE_CTL + 1
   SO_GET_REVISION_MATCH = BASE_CTL + 2
   SO_GET_REVISION_TARGET = BASE_CTL + 3

class XTablesIP6(XTables):
   af = socket.AF_INET6
   xtge = XTGetEntries_IP6
   xtgi = XTGetInfo_IP6
   sol = SOL_IPV6
   # Taken from /usr/include/linux/netfilter_ipv6/ip6_tables.h
   BASE_CTL = 64
   SO_GET_INFO = BASE_CTL
   SO_GET_ENTRIES = BASE_CTL + 1
   SO_GET_REVISION_MATCH = BASE_CTL + 4
   SO_GET_REVISION_TARGET = BASE_CTL + 5

class XTablesARP(XTables):
   af = socket.AF_INET
   xtge = XTGetEntries_ARP
   xtgi = XTGetInfo_ARP
   sol = SOL_IP
   # taken from /usr/include/linux/netfilter_arp/arp_tables.h
   BASE_CTL = 96
   SO_GET_INFO = BASE_CTL
   SO_GET_ENTRIES = BASE_CTL + 1
   SO_GET_REVISION_MATCH = BASE_CTL + 2
   SO_GET_REVISION_TARGET = BASE_CTL + 3


class XTablesPoller:
   def __init__(self, ed, interval, xt=None, tables=('filter',)):
      if (xt is None):
         xt = XTablesIP()
      self.em_xtentries = EventMultiplexer(self)
      self.ed = ed
      self.xt = xt
      self.tables = tables
      self.timer = Timer(ed, interval, self.xt_poll, persistence=True, align=True, parent=self)
   
   def xt_poll(self):
      for table in self.tables:
         self.em_xtentries(self.xt.table_read(table))


if (__name__ == '__main__'):
   import pprint
   import sys

   import logging
   logger = logging.getLogger()
   log = logger.log
   logger.setLevel(0)
   formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
   handler_stderr = logging.StreamHandler()
   handler_stderr.setLevel(30)
   handler_stderr.setFormatter(formatter)
   logger.addHandler(handler_stderr)

   try:
      tablename = sys.argv[1]
   except IndexError:
      tablename = 'filter'
   
   print('=== Testing: XTables ===')
   for cls in (XTablesIP, XTablesIP6, XTablesARP):
      NI = cls()
      print('------------------------------------------------ cls: %s' % (cls,))
      xtgec = NI.table_read(tablename)
      xtge = xtgec.xtge
      print('----- retrieved: %f %f' % (xtgec.ts_low, xtgec.ts_high))
      for ie in xtge.entries:
         #pprint.pprint((ie.fields_get(), ie.target_get()))
         print(int(ie.is_chainstart()), ie.target, ie.target_data, ie.counter_packets, ie.counter_bytes)
      pprint.pprint(xtge.get_chains())
      NI.close()
   
   print('\n=== Testing: XTablesPoller ===')
   from gonium.fd_management import EventDispatcherPoll
   
   def quit_print(*args, **kwargs):
      print((args, kwargs))
      ed.shutdown()

   ed = EventDispatcherPoll()
   xtp = XTablesPoller(ed, 1)
   xtp.em_xtentries.EventListener(quit_print)
   ed.event_loop()
   
   print('\n=== All tests passed. ===')

