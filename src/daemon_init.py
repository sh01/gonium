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

import os
import sys
import logging
import warnings
from warnings import showwarning as showwarning_orig

def daemon_init(stdout_filename='log/stdout', stderr_filename='log/stderr', setsid=True):
   fork_result = os.fork()
   if (fork_result != 0):
      print 'Started as %s.' % (fork_result,)
      sys.exit(0)
   
   if (setsid):
      os.setsid()

   #Close stdin, stdout and stderr, and optionally replace the latter ones with file-streams.
   os.close(sys.stdin.fileno())
   sys.stdin.close()
   os.close(sys.stdout.fileno())
   sys.stdout.close()
   os.close(sys.stderr.fileno())
   sys.stderr.close()
   if (stdout_filename):
      sys.stdout = file(stdout_filename, 'a+', 1)
   if (stderr_filename):
      sys.stderr = file(stderr_filename, 'a+', 0)
   
   sys.__stderr__ = sys.__stdout__ = sys.__stdin__ = sys.stdin = None


logger_warnings = logging.getLogger('warnings')

def showwarning_logging(message, category, filename, lineno, file=None):
   if not (file is None):
      showwarning_orig(message, category, filename, lineno, file)
      
   logger_warnings.warn(formatwarning(message, category, filename, lineno))

def warnings_redirect_logging():
   warnings.showwarning = showwarning_logging

