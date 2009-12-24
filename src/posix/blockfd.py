#!/usr/bin/env python
#Copyright 2009 Sebastian Hagen
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

# Frontend module for dealing with posix block fds. Posix block fds (usually
# called 'fast fds') are fds on block-like file-likes; i.e. either real files
# or block devices. read() and write() accesses to fds of this kind have the
# annoying property of tending to block the calling process.

import logging

from ._blockfd import DataTransferDispatcher as _DataTransferDispatcher, \
   DataTransferRequest

class DataTransferDispatcher(_DataTransferDispatcher):
   logger = logging.getLogger('DataTransferDispatcher')
   log = logger.log
   __doc__ = _DataTransferDispatcher.__doc__
   
   def __init__(self, *args, **kwargs):
      super().__init__()
      self.fw = None
   
   def attach_ed(self, ed):
      """Attach to EventDispatcher by signal pipe."""
      if not (self.fw is None):
         raise ValueError('Already attached.')
      fw = ed.fd_wrap(self.fileno(), fl=self)
      fw.read_r()
      fw.process_readability = self.process_results
   
   def detach_ed(self):
      """Detach from EventDispatcher."""
      if (self.fw is None):
         raise ValueError('Not currently attached.')
      self.fw.read_u()
      self.fw = None
   
   def new_req(self, src, dst, cb, length, src_off=None, dst_off=None):
      """Return new DTR."""
      return DataTransferRequest(self, src, dst, src_off, dst_off, length, cb)
   
   def new_req_fd2mem(self, src, dst, cb, length=None, src_off=None):
      """Return new fd2mem DTR."""
      if (length is None):
         length = len(dst)
      return DataTransferRequest(self, src, dst, src_off, None, length, cb)
   
   def new_req_mem2fd(self, src, dst, cb, length=None, dst_off=None):
      """Return new mem2fd DTR."""
      if (length is None):
         length = len(src)
      return DataTransferRequest(self, src, dst, None, dst_off, length, cb)
   
   def new_req_mem2mem(self, src, dst, cb, length=None):
      """Return new mem2mem DTR."""
      if (length is None):
         length = len(src)
         if (length != len(dst)):
            raise ValueError('len(src) != len(dst) is invalid without explicit length.')
      return DataTransferRequest(self, src, dst, None, None, length, cb)
   
   def process_results(self):
      """Process pending results and call callback for each."""
      results = self.get_results()
      for dtr in results:
         cb = dtr.opaque
         try:
            cb(dtr)
         except Exception as exc:
            self.log(40, 'Callback {0!a} extracted from DTR {1!a} threw exception:'.format(cb, dtr), exc_info=True)
         if (dtr.get_missing_byte_count() == 0):
            # _blockfd has no explicit support for gc cycle detection, so we
            # hack around it here.
            dtr.opaque = None


# -------------------------------------------------- test cases
class _ModuleSelfTester:
   log = logging.getLogger('_BlockFDSelfTester').log
   reqcount = 1024
   bs = 102403
   flen = bs*reqcount
   
   @staticmethod
   def get_rand_offpairs(bs, count):
      from random import shuffle
      offs = range(0, bs*count, bs)
      offs_r = list(offs)
      shuffle(offs_r)
      return tuple(zip(offs,offs_r))

   def wait_full(self, reqcount):
      from select import select
      rd_count = 0
      while (rd_count < reqcount):
         select([self.dtd],[],[])
         reqs_done = self.dtd.get_results()
         rd_count += len(reqs_done)

   def hash_data(self, data, ph=None):
      from hashlib import sha1
      h = sha1()
      h.update(data)
      
      rv = h.digest()
      if (ph is None):
         return rv
      
      if (rv == ph):
         self.log(20, '...pass.')
      else:
         self.log(50, '...FAIL!')
         raise Exception('Hash mismatch.')
      
      return h.digest()

   def hashfile(self, f, ph=None):
      f.seek(0)
      return self.hash_data(f.read(), ph)

   def run_tests(self):
      self.tests_prep()
      self.rt_socks(True, True)
      self.rt_socks(False)
      self.rt_backend()
      self.rt_backend()
      self.rt_socks(False)
      self.rt_socks(True, False)
      self.rt_socks(True, False)
      self.rt_socks(True, True)
      
      self.log(20, 'All done.')

   def tests_prep(self):
      self.log(20, 'DTD init ...')
      self.dtd = dtd = DataTransferDispatcher(50)
      self.log(20, 'Opening files ...')
      furand = open('/dev/urandom', 'rb')
      self.f1 = f1 = open('__t1.tmp', 'w+b')
      self.f2 = f2 = open('__t2.tmp', 'w+b')
      f1.seek(0)
      f2.seek(0)
      f1.truncate()
      f2.truncate()
   
      self.log(20, 'Copying urandom to tempfile, dst offsets only.')
      off = 0
      for i in range(self.reqcount):
         dtr = DataTransferRequest(dtd, furand, f1, None, off, self.bs, None)
         off += self.bs
         dtr.queue()
   
      self.log(20, 'Waiting for request completion ...')
      self.wait_full(self.reqcount)
   
      if (f1.seek(0,2) != self.flen):
         self.log(50, '...size check: FAIL.')
         raise Exception()
   
      self.log(20, 'Hashing first copy of data ...')
      self.hd1 = self.hashfile(f1)
      
      from ..fdm.ed import ED_get
      self.ed = ED_get()()
      self.dtd.attach_ed(self.ed)
 
   def rt_backend(self):
      import random
      dtd = self.dtd
      hd1 = self.hd1
      f1 = self.f1
      f2 = self.f2
      reqcount = self.reqcount
      bs = self.bs
   
      self.log(20, 'Error test: thread overkill')
      try:
         DataTransferDispatcher(500000)
      except:
         self.log(20, '...pass.')
      else:
         raise Exception('Failed to raise exception.')
   
      self.log(20, 'Testing fd2fd copy; 2 block fds, by offset, random start order.')
      off = 0
      reqs = []
      for i in range(reqcount):
         dtr = DataTransferRequest(dtd, f1, f2, off, off, bs, None)
         off += bs
         reqs.append(dtr)
   
      random.shuffle(reqs)
      for req in reqs:
         req.queue()
   
      self.wait_full(reqcount)
      f2.flush()
      self.log(20, 'Copy done. Verifying data ...')
      hd2 = self.hashfile(f2, hd1)
   
      self.log(20, 'Doing linear no-offset copy ...')
      f1.seek(0)
      f2.seek(0)
      f2.truncate()
      for i in range(reqcount):
         dtr = DataTransferRequest(dtd, f1, f2, None, None, bs, None)
         dtr.queue()
         self.wait_full(1)
   
      self.log(20, 'Copy done. Verifying data ...')
      hd3 = self.hashfile(f2, hd1)
   
      f2.seek(0)
      f2.truncate()
   
      self.log(20, 'Doing offset randomizing copy ...')
      offpairs = self.get_rand_offpairs(bs, reqcount)

      for (off1, off2) in offpairs:
         dtr = DataTransferRequest(dtd, f1, f2, off1, off2, bs, None)
         dtr.queue()
   
      self.wait_full(len(offpairs))
      self.log(20, 'Copy done. Verifying data ...')
   
      for (off1, off2) in offpairs:
         f1.seek(off1)
         f2.seek(off2)
         if (f1.read(bs) != f2.read(bs)):
            self.log(50, 'FAIL.')
            raise Exception
   
      self.log(20, '...pass.')
      self.log(20, 'Testing fd2mem ...')
      ba = bytearray(self.flen)
      mv = memoryview(ba)
   
      for (off1, off2) in offpairs:
         dtr = DataTransferRequest(dtd, f2, mv[off1:], off2, None, bs, None)
         dtr.queue()
   
      self.wait_full(len(offpairs))
   
      f2.seek(0)
      f2.truncate()
   
      self.log(20, 'Testing mem2fd by offset...')
      offs = range(0, bs*reqcount, bs)
      
      for off1 in offs:
         dtr = DataTransferRequest(dtd, mv[off1:], f1, None, off1, bs, None)
         dtr.queue()
   
      self.wait_full(len(offs))
      self.log(20, 'Bidirectional copy done. Verifying data ...')
      hd4 = self.hashfile(f1, hd1)
   
      self.log(20, 'Testing mem2fd / fd2mem / mem2mem, the former two in no-offset mode ...')
      f2.seek(0)
      f2.truncate()
      f1.seek(0)
   
      ba = bytearray(self.flen)
      ba2 = bytearray(self.flen)
      mv = memoryview(ba)
      mv2 = memoryview(ba)
      
      for off1 in offs:
         dtr = DataTransferRequest(dtd, f1, mv[off1:], None, None, bs, None)
         dtr.queue()
   
      self.wait_full(len(offs))
   
      for off1 in offs:
         dtr = DataTransferRequest(dtd, mv[off1:], mv2[off1:], None, None, bs, None)
         dtr.queue()
   
      self.wait_full(len(offs))
   
      for off1 in offs:
         dtr = DataTransferRequest(dtd, mv[off1:], f2, None, None, bs, None)
         dtr.queue()
   
      self.wait_full(len(offs))
      self.log(20, 'Bidirectional copy done. Verifying data ...')
      hd5 = self.hashfile(f1, hd1)
   
      self.log(20, 'Testing init sanity check code.')
      for args in (
            (mv[:20], mv, None, None, 21, None),
            (mv, mv[:20], None, None, 21, None),
            (f2, mv[:20], None, None, 21, None),
            (mv[:20], f2, None, None, 21, None),
         ):
         try:
            dtr = DataTransferRequest(dtd, *args)
         except:
            continue
         raise Exception('Failed to get exception from {0!a}.'.format(args))
      self.log(20, '...pass.')
   
      self.log(20, 'Testing I/O error reporting.')
      f1.seek(0)
      f2.seek(0)
      f2.truncate()
   
      badfd = 2**14
      for args in (
            (badfd, mv, None, None, 21, None),
            (mv, badfd, None, None, 21, None),
            (badfd, f2, None, None, 21, None),
            (f1, badfd, None, None, 21, None),
         ):
      
         dtr = DataTransferRequest(dtd, *args)
         dtr.queue()
         self.wait_full(1)
         try:
            rv = dtr.get_errors()
         except Exception as exc:
            self.log(15, 'Correctly got exception {0!a}.'.format(exc))
            continue
      
         raise Exception("DTR with args {0!a} failed to fail.".format(args))
   
      for args in (
            (f2, mv, None, None, 21, None),
            (f2, f1, None, None, 21, None),
            (f1, mv, self.flen-1024**2*5-293, None, 1024**2*10, None),
         ):
         dtr = DataTransferRequest(dtd, *args)
         dtr.queue()
         self.wait_full(1)
         if (not dtr.get_missing_byte_count()):
            raise Exception("DTR with args {0!a} failed to fail.".format(args))
   
   def rt_socks(self, use_ssl, defer_ssl_send=False):
      import random
      from collections import deque
      from ..fdm.stream import AsyncDataStream, AsyncSockServer
      
      if (use_ssl):
         import tempfile
         from ._blockfd_tcd import dummy_ca_crt, dummy_ca_key
         
         caf_crt = tempfile.NamedTemporaryFile()
         caf_key = tempfile.NamedTemporaryFile()
         caf_crt.write(dummy_ca_crt)
         caf_crt.flush()
         caf_key.write(dummy_ca_key)
         caf_key.flush()
      
      self.log(20, 'Socket test: file2sock: randomized regular / dtded in random order (SSL: {0}({1})).'.format(use_ssl, defer_ssl_send))
      self.log(20, 'Setting up sockets ...')
      s_s = AsyncSockServer(self.ed, ('127.0.0.1',0))
      saddr = s_s.sock.getsockname()
      s_c1 = None
      s_c2 = None
      s_c1_fw = None
      
      ed = self.ed
      count = self.reqcount
      bs = self.bs
      offpairs = self.get_rand_offpairs(bs, count)
      flen = bs*count
      f2 = self.f2
      
      ba = bytearray(flen)
      mv = memoryview(ba)
      i = 0
      s2s_transfers = deque()
      
      def pi(data):
         nonlocal i
         i_diff = len(data)
         mv[i:i+i_diff] = data
         i += i_diff
         s_in.discard_inbuf_data()
         if (i == flen):
            ed.shutdown()
         #print('X1: ', i, i_diff, flen-i)
      
      def s2s_copy_fp(dtr):
         nonlocal s_c2
         if (dtr.get_missing_byte_count() == 0):
            s2s_transfers.popleft()
            if (not s2s_transfers):
               s_c2.close()
               s_c2 = None
         
         s_c1_fw.read_r()
      
      def s2s_copy():
         if not (s2s_transfers):
            if (s_c1.recv(1024) == b''):
               s_c1_fw.close()
               return
            
            raise Exception('Got too much data to copy.')
         s2s_transfers[0].queue()
         s_c1_fw.read_u()
      
      def cp(sock, addressinfo):
         nonlocal s_c1, s_c2, s_c1_fw
         if (s_c1 is None):
            s_c1 = sock
            s_c1.setblocking(0)
            ed.shutdown()
            return
         s_c2 = sock
         s_c1_fw = ed.fd_wrap(s_c1.fileno(), fl=s_c1)
         s_c1_fw.process_readability = s2s_copy
         s_c1_fw.read_r()
         ed.shutdown()
         
      
      s_s.connect_process = cp
      s_out = AsyncDataStream.build_sock_connect(self.ed, saddr)
      
      ed.event_loop()
      
      def do_copy():
         for (off1, off2) in offpairs:
            if (random.randint(0,1) and 0):
               s_out.send_bytes_from_file(self.dtd, self.f1, off2, bs)
            else:
               self.f1.seek(off2)
               s_out.send_bytes((self.f1.read(bs),))
         
            if (not use_ssl):
               s2s_transfers.append(self.dtd.new_req(s_c1, s_c2, s2s_copy_fp, bs, None, None))
      
      sslh_done = 0
      def dcisd():
         nonlocal sslh_done
         if (not defer_ssl_send):
            return
         if (sslh_done < 1):
            sslh_done += 1
         do_copy()
      
      if (use_ssl):
         s_in = AsyncDataStream(self.ed, s_c1)
         s_in.process_input = pi
      
      else:
         s_in = AsyncDataStream.build_sock_connect(self.ed, saddr)
         s_in.process_input = pi
         ed.event_loop()
      
      if (use_ssl):
         s_in.do_ssl_handshake(dcisd, server_side=True,
            certfile=caf_crt.name, keyfile=caf_key.name)
         s_out.do_ssl_handshake(dcisd)
      
      self.log(20, 'Transferring data ...')
      
      if (not use_ssl):
         do_copy()
      elif (not defer_ssl_send):
         self.ed.set_timer(1024, do_copy, interval_relative=False)
      
      ed.event_loop()
      
      s_s._fw.close()
      s_in.close()
      s_out.close()
      
      ba2 = bytearray(flen)
      mv2 = memoryview(ba2)
      for (off1, off2) in offpairs:
         mv2[off2:off2+bs] = mv[off1:off1+bs]
      
      ba = None
      mv = None
      
      self.log(20, 'Done. Verifying equality ...')
      
      hd2 = self.hash_data(mv2, self.hd1)


if (__name__ == '__main__'):
   import sys
   logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
      stream=sys.stderr, level=logging.DEBUG)
   st = _ModuleSelfTester()
   st.run_tests()
