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

# This file is an interface to libcap2, providing functionality originally
# specified by the withdrawn POSIX.1e draft spec.
# Depends on ctypes and libcap2.

import ctypes

libcap = ctypes.CDLL('libcap.so.2')
libcap.cap_init.argtypes = ()
libcap.cap_init.restype = ctypes.c_void_p

libcap.cap_free.argtypes = (ctypes.c_void_p,)

libcap.cap_dup.argtypes = (ctypes.c_void_p,)
libcap.cap_dup.restype = ctypes.c_void_p

libcap.cap_from_text.argtypes = (ctypes.c_char_p,)
libcap.cap_from_text.restype = ctypes.c_void_p

libcap.cap_to_text.argtypes = (ctypes.c_void_p, ctypes.c_void_p)
libcap.cap_to_text.restype = ctypes.c_void_p

libcap.cap_get_proc.argtypes = ()
libcap.cap_get_proc.restpye = ctypes.c_void_p

libcap.cap_set_proc.argtypes = (ctypes.c_void_p,)

class CapError(StandardError):
   pass

class CapStateError(CapError):
   pass

class Capability:
   """Class encapsulating a pointer to a cap_t memory region, for safety and
      ease of use."""
   def __init__(self, cap_p):
      """Build instance from existing cap_p pointer."""
      self.cap_p = cap_p
      
   @classmethod
   def from_text(cls, s):
      """Build instance from string using cap_from_text()"""
      cap_p = libcap.cap_from_text(s)
      if not (cap_p):
         raise CapError('cap_from_text() failed.')
      return cls(cap_p)
   
   @classmethod 
   def get_proc(cls):
      """Build instance from current process capabilities using cap_get_proc()"""
      cap_p = libcap.cap_get_proc()
      return cls(cap_p)
   
   def test_capp(self):
      if (self.cap_p is None):
         raise CapStateError("%r is currently invalid." % (self,))
   
   def dup(self):
      self.test_capp()
      cap_p = libcap.cap_dup(self.cap_p)
      if not (cap_p):
         raise CapError('cap_dup() failed.')
      return self.__class__(cap_p)
   
   def copy(self):
      return self.dup()
   
   def set_proc(self):
      """Set capabilities of this process to those specified by Capability instance"""
      self.test_capp()
      i = libcap.cap_set_proc(self.cap_p)
      if (i):
         raise CapError('cap_set_proc() failed.')
      
   def to_text(self):
      """Convert to string using cap_to_text()."""
      self.test_capp()
      s_p = ctypes.c_char_p(libcap.cap_to_text(self.cap_p, ctypes.c_void_p(0)))
      if not (s_p):
         raise CapError('cap_to_text() failed.')
      rv = s_p.value
      libcap.cap_free(s_p)
      return rv
   
   def __repr__(self):
      return '%s.from_text(%r)' % (self.__class__.__name__, self.to_text())
   
   def _dest(self):
      if (self.cap_p is None):
         return
      libcap.cap_free(self.cap_p)
      self.cap_p = None
   
   def __del__(self):
      return self._dest()


if (__name__ == '__main__'):
   # Here there be self-tests.
   print('=== Cap-read test ===')
   cap = Capability.get_proc()
   cap2 = cap.copy()
   cap._dest()
   cap = cap2
   print('Process capabilities: %s' % (cap,))
   print('=== Cap-write test ===')
   print('Attempting to set ...')
   cap.set_proc()
   print('Success.')
   print('=== All tests passed ===')
