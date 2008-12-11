#!/usr/bin/env python2.3
#Copyright 2004 Sebastian Hagen
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

import time

fmt_TIME = '%Y-%m-%d %H:%M:%S'

def seconds_hr_absolute(unixtime):
   return time.strftime(fmt_TIME, time.localtime(unixtime))


def seconds_hr_relative(seconds, precision=0):
   output_list = []
   for (name, length) in (('days', 86400), ('hours', 3600), ('minutes', 60)):
      element_count = seconds / length
      seconds = seconds % length
      if (element_count >= 1):
         if (element_count == 1):
            #strip last letter
            name = name[:-1]
         output_list.append('%d %s' % (element_count, name))


   if (seconds == 1):
      name = 'second'
   else:
      name = 'seconds'
   
   output_list.append('%.*f %s' % (precision, seconds, name))

   return ' '.join(output_list)
