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

from ._slowfd import DataTransferDispatcher as _DataTransferDispatcher, \
   DataTransferRequest


# -------------------------------------------------- test cases
def _st_wait_full(dtd, reqcount):
   from select import select
   rd_count = 0
   while (rd_count < reqcount):
      select([dtd],[],[])
      reqs_done = dtd.get_results()
      rd_count += len(reqs_done)

def _st_hashfile(f, ph=None, log=None):
   from hashlib import sha1
   
   f.seek(0)
   h = sha1()
   h.update(f.read())
   rv = h.digest()

   if (ph is None):
      return rv
   
   if (rv == ph):
      log(20, '...pass.')
   else:
      log(50, '...FAIL!')
      raise Exception()
   
   return rv
 
def _main():
   """Perform module selftests."""
   import sys
   import random
   import logging
   logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
      stream=sys.stderr, level=logging.DEBUG)
   
   log = logging.getLogger().log
   
   log(20, 'Error test: thread overkill')
   try:
      _DataTransferDispatcher(500000)
   except:
      log(20, '...pass.')
   else:
      raise Exception('Failed to raise exception.')
   
   log(20, 'DTD init ...')
   dtd = _DataTransferDispatcher(50)
   log(20, 'Opening files ...')
   furand = open('/dev/urandom', 'rb')
   f1 = open('__t1.tmp', 'w+b')
   f2 = open('__t2.tmp', 'w+b')
   f1.truncate(0)
   f2.truncate(0)
   fnull = open('/dev/null', 'wb')
   
   reqcount = 1024
   bs = 102400
   log(20, 'Copying urandom to tempfile, dst offsets only.')
   off = 0
   for i in range(reqcount):
      dtr = DataTransferRequest(dtd, furand, f1, None, off, bs, None)
      off += bs
      dtr.queue()
   
   log(20, 'Waiting for request completion ...')
   _st_wait_full(dtd, reqcount)
   
   flen = bs*reqcount
   
   if (f1.seek(0,2) != flen):
      log (50, '...size check: FAIL.')
      raise Exception()
   
   log(20, 'Hashing first copy of data ...')
   hd1 = _st_hashfile(f1)
   
   log(20, 'Testing fd2fd copy; 2 slow fds, by offset, random start order.')
   off = 0
   reqs = []
   for i in range(reqcount):
      dtr = DataTransferRequest(dtd, f1, f2, off, off, bs, None)
      off += bs
      reqs.append(dtr)
   
   random.shuffle(reqs)
   for req in reqs:
      req.queue()
   
   _st_wait_full(dtd, reqcount)
   f2.flush()
   log(20, 'Copy done. Verifying data ...')
   hd2 = _st_hashfile(f2, hd1, log)
   
   log(20, 'Doing linear no-offset copy ...')
   f1.seek(0)
   f2.seek(0)
   f2.truncate()
   for i in range(reqcount):
      dtr = DataTransferRequest(dtd, f1, f2, None, None, bs, None)
      dtr.queue()
      _st_wait_full(dtd, 1)
   
   log(20, 'Copy done. Verifying data ...')
   hd3 = _st_hashfile(f2, hd1, log)
   
   f2.seek(0)
   f2.truncate()
   
   log(20, 'Doing offset randomizing copy ...')
   offs = range(0, bs*reqcount, bs)
   offs_r = list(offs)
   random.shuffle(offs_r)
   offpairs = tuple(zip(offs,offs_r))

   for (off1, off2) in offpairs:
      dtr = DataTransferRequest(dtd, f1, f2, off1, off2, bs, None)
      dtr.queue()
   
   _st_wait_full(dtd, len(offs))
   log(20, 'Copy done. Verifying data ...')
   
   
   for (off1, off2) in offpairs:
      f1.seek(off1)
      f2.seek(off2)
      if (f1.read(bs) != f2.read(bs)):
         log(20, 'FAIL.')
         raise Exception
   
   log(20, '...pass.')
   log(20, 'Testing fd2mem ...')
   ba = bytearray(flen)
   mv = memoryview(ba)
   
   for (off1, off2) in offpairs:
      dtr = DataTransferRequest(dtd, f2, mv[off1:], off2, None, bs, None)
      dtr.queue()
   
   _st_wait_full(dtd, len(offs))
   
   f2.seek(0)
   f2.truncate()
   
   log(20, 'Testing mem2fd by offset...')
   for off1 in offs:
      dtr = DataTransferRequest(dtd, mv[off1:], f1, None, off1, bs, None)
      dtr.queue()
   
   _st_wait_full(dtd, len(offs))
   log(20, 'Bidirectional copy done. Verifying data ...')
   hd4 = _st_hashfile(f1, hd1, log)
   
   log(20, 'Testing mem2fd / fd2mem / mem2mem, the former two in no-offset mode ...')
   f2.seek(0)
   f2.truncate()
   f1.seek(0)
   
   ba = bytearray(flen)
   ba2 = bytearray(flen)
   mv = memoryview(ba)
   mv2 = memoryview(ba)
   
   for off1 in offs:
      dtr = DataTransferRequest(dtd, f1, mv[off1:], None, None, bs, None)
      dtr.queue()
   
   _st_wait_full(dtd, len(offs))
   
   for off1 in offs:
      dtr = DataTransferRequest(dtd, mv[off1:], mv2[off1:], None, None, bs, None)
      dtr.queue()
   
   _st_wait_full(dtd, len(offs))
   
   for off1 in offs:
      dtr = DataTransferRequest(dtd, mv[off1:], f2, None, None, bs, None)
      dtr.queue()
   
   _st_wait_full(dtd, len(offs))
   log(20, 'Bidirectional copy done. Verifying data ...')
   hd5 = _st_hashfile(f1, hd1, log)
   
   log(20, 'Testing init sanity check code.')
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
   log(20, '...pass.')
   
   log(20, 'Testing I/O error reporting.')
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
      _st_wait_full(dtd, 1)
      try:
         rv = dtr.get_errors()
      except Exception as exc:
         log(15, 'Correctly got exception {0!a}.'.format(exc))
         continue
      
      raise Exception("DTR with args {0!a} failed to fail.".format(args))
   
   for args in (
         (f2, mv, None, None, 21, None),
         (f2, f1, None, None, 21, None),
         (f1, mv, flen-1024**2*5-293, None, 1024**2*10, None),
      ):
      dtr = DataTransferRequest(dtd, *args)
      dtr.queue()
      _st_wait_full(dtd, 1)
      if (not dtr.get_missing_byte_count()):
         raise Exception("DTR with args {0!a} failed to fail.".format(args))
   
   log(20, 'All done.')


if (__name__ == '__main__'):
   _main()
