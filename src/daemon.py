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

_logger_warnings = logging.getLogger('warnings')

def daemon_fork(stdout_filename=b'/dev/null', stderr_filename=b'/dev/null',
      setsid=True, warnings_redirect=True, pidfile=None):
   
   if (pidfile):
      pidfile.unlock()
   
   fork_result = os.fork()
   if (fork_result != 0):
      print('Started as {0}.'.format(fork_result,))
      sys.exit(0)
   
   if (pidfile):
      pidfile.lock()
   
   if (setsid):
      os.setsid()

   #Close stdin, stdout and stderr, and optionally replace the latter ones with file-streams.
   os.close(sys.stdin.fileno())
   sys.stdin.close()
   os.close(sys.stdout.fileno())
   sys.stdout.close()
   os.close(sys.stderr.fileno())
   sys.stderr.close()
   
   if (warnings_redirect):
      warnings_redirect_logging()
   
   if (stdout_filename):
      sys.stdout = open(stdout_filename, 'a+', 1)
   if (stderr_filename):
      sys.stderr = open(stderr_filename, 'a+', 0)
   
   sys.__stderr__ = sys.__stdout__ = sys.__stdin__ = sys.stdin = None


def _showwarning_logging(message, category, filename, lineno, file=None, *args,
      **kwargs):
   if not (file is None):
      showwarning_orig(message, category, filename, lineno, file, *args, **kwargs)
      return
      
   _logger_warnings.warn(formatwarning(message, category, filename, lineno,
      *args, **kwargs))

def _warnings_redirect_logging():
   warnings.showwarning = _showwarning_logging

