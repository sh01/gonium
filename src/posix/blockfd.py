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
   
   def __init__(self, *args, **kwargs):
      super().__init__()
      self.fw = None
   
   def attach_ed(self, ed):
      """Attach to EventDispatcher by signal pipe."""
      if not (self.fw is None):
         raise ValueError('Already attached.')
      fw = ed.fd_wrap(self.fileno(), self)
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
      return DataTransferRequest(self, src, dst, src_off, None, length, cb)
   
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
            cb(result)
         except Exception as exc:
            self.log(40, 'Callback {0!a} extracted from DTR {1!a} threw exception:'.format(cb, dtr), exc_info=True)
         cb.opaque = None


# -------------------------------------------------- test cases
class _SelfTester:
   log = logging.getLogger().log
   reqcount = 1024
   bs = 102400
   flen = bs*reqcount

   def wait_full(self, reqcount):
      from select import select
      rd_count = 0
      while (rd_count < reqcount):
         select([self.dtd],[],[])
         reqs_done = self.dtd.get_results()
         rd_count += len(reqs_done)

   def hashfile(self, f, ph=None):
      from hashlib import sha1
   
      f.seek(0)
      h = sha1()
      h.update(f.read())
      rv = h.digest()

      if (ph is None):
         return rv
   
      if (rv == ph):
         self.log(20, '...pass.')
      else:
         self.log(50, '...FAIL!')
         raise Exception()
   
      return rv

   def run_tests(self):
      self.tests_prep()
      self.rt_backend()

   def tests_prep(self):
      self.log(20, 'DTD init ...')
      self.dtd = dtd = DataTransferDispatcher(50)
      self.log(20, 'Opening files ...')
      furand = open('/dev/urandom', 'rb')
      self.f1 = f1 = open('__t1.tmp', 'w+b')
      self.f2 = f2 = open('__t2.tmp', 'w+b')
      f1.truncate(0)
      f2.truncate(0)
   
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
      offs = range(0, bs*reqcount, bs)
      offs_r = list(offs)
      random.shuffle(offs_r)
      offpairs = tuple(zip(offs,offs_r))

      for (off1, off2) in offpairs:
         dtr = DataTransferRequest(dtd, f1, f2, off1, off2, bs, None)
         dtr.queue()
   
      self.wait_full(len(offs))
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
   
      self.wait_full(len(offs))
   
      f2.seek(0)
      f2.truncate()
   
      self.log(20, 'Testing mem2fd by offset...')
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
   
      self.log(20, 'All done.')


if (__name__ == '__main__'):
   import sys
   logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
      stream=sys.stderr, level=logging.DEBUG)
   st = _SelfTester()
   st.run_tests()
