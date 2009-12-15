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

from ._asyncfd2fd import DataTransferDispatcher as _DataTransferDispatcher, \
   DataTransferRequest


def main():
   from select import select
   
   print('DTD init ...')
   dtd = _DataTransferDispatcher(50)
   print('Opening files ...')
   f1 = open('/dev/urandom', 'rb')
   f2 = open('/dev/null', 'wb')
   
   reqcount = 1000
   print('Request init ...')
   for i in range(reqcount):
      dtr = DataTransferRequest(dtd, f1, f2, None, None, 102400, None)
      dtr.queue()
   
   rd_count = 0
   print('Waiting for request completion ...')
   while (rd_count < reqcount):
      select([dtd],[],[])
      reqs_done = dtd.get_results()
      rd_count += len(reqs_done)
      print(rd_count)
   
   print('All done.')


if (__name__ == '__main__'):
   main()
