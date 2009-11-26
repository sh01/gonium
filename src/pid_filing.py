#!/usr/bin/env python
#Copyright 2004,2008 Sebastian Hagen
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

import sys
import os
import fcntl

class PidFile:
   def __init__(self, filename:bytes=None):
      """Open pid-file."""
      if (filename is None):
         argv0 = sys.argv[0]
         if (isinstance(argv0, str)):
            # Get rid of silly unicode names
            argv0 = argv0.encode()
         filename = os.path.basename(argv0) + b'.pid'
      if (os.path.exists(filename)):
         mode = 'r+b'
      else:
         mode = 'wb'
      
      # The feature allowing for calling open() on bytes filenames was added
      # somewhere between CPython 3.0-rc1 and -rc3. This version is written
      # for 3.0 final, so using it should be fine.
      self.filename = filename
      self.file = open(filename, mode)
   
   def lock(self, else_die:bool=False):
      """Acquire lock on pid file; if successful, write our pid to it. If 
         the optional argument is specified and True, any IOErrors will 
         be caught and turned into SystemExits."""
      try:
         fcntl.lockf(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
      except IOError:
         if (else_die):
            print('Our pid-file {0} is already locked, aborting.'.format(self.filename,))
            sys.exit(0)
         raise
      self.file.seek(0)
      self.file.write(ascii(os.getpid()).encode('ascii'))
      self.file.truncate()
   def unlock(self):
      """Release lock on pid file."""
      fcntl.lockf(self.file.fileno(), fcntl.LOCK_UN)

