#!/usr/bin/env python
#Copyright 2015 Sebastian Hagen
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

import ctypes

lc = ctypes.CDLL('')

inotify_init = lc.inotify_init
inotify_init.argtypes = ()

inotify_add_watch = lc.inotify_add_watch
inotify_add_watch.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)

inotify_rm_watch = lc.inotify_rm_watch
inotify_rm_watch.argtypes = (ctypes.c_int, ctypes.c_int)

IN_ACCESS = 0x1
IN_MODIFY = 0x2
IN_ATTRIB = 0x4
IN_CLOSE_WRITE = 0x8
IN_CLOSE_NOWRITE = 0x10
IN_OPEN = 0x20
IN_MOVED_FROM = 0x40
IN_MOVED_TO = 0x80
IN_CREATE = 0x100
IN_DELETE = 0x200
IN_DELETE_SELF = 0x400
IN_MOVE_SELF = 0x800

IN_UNMOUNT = 0x00002000
IN_Q_OVERFLOW = 0x00004000
IN_IGNORED = 0x00008000

IN_ONLYDIR = 0x01000000
IN_DONT_FOLLOW = 0x02000000
IN_EXCL_UNLINK = 0x04000000
IN_MASK_ADD = 0x20000000
IN_ISDIR = 0x40000000
IN_ONESHOT = 0x80000000


class InotifyWatch:
  INT_SZ = ctypes.sizeof(ctypes.c_int)
  HDR_SZ = INT_SZ + 12
  def __init__(self, ed):
    from fcntl import fcntl, F_SETFL
    from os import O_NONBLOCK

    fd = inotify_init()
    if (fd < 0):
      raise SystemError('inotify_init() failed.')
    fcntl(fd, F_SETFL, O_NONBLOCK)

    # The filelike will handle closing our fd on destruction.
    self._fl = fl = open(fd, 'rb')
    self._fw = fw = ed.fd_wrap(fd, fl=fl)
    self._fw.process_readability = self._process_readability
    self._fw.process_close = self._process_close
    fw.read_r()

  def process_event(self, *args, **kwargs):
    raise NotImplementedError('process_event(*{!r}, **{!r})'.format(args, kwargs))

  def add_watch(self, pathname, mask):
    wd = inotify_add_watch(self._fw.fd, pathname, mask)
    if (wd < 0):
      raise ValueError('inotify_add_watch(, {!r}, {!r}) failed.'.format(pathname, mask))
    return wd

  def _process_readability(self):
    from sys import byteorder as bo
    fb = int.from_bytes
    
    while True:
      hdr = self._fl.read(self.HDR_SZ)
      if (hdr is None):
        break
    
      wd = fb(hdr[:self.INT_SZ], bo)
      off = self.INT_SZ
      mask = fb(hdr[off:off+4], bo)
      off += 4
      cookie = fb(hdr[off:off+4], bo)
      off += 4
      length = fb(hdr[off:off+4], bo)

      name = self._fl.read(length).rstrip(b'\x00')
      self.process_event(wd, mask, cookie, name)

  def process_close(self):
    pass

  def _process_close(self):
    self.process_close()
    self._fw = None


def main():
  from . import ED_get
  from sys import argv

  ed = ED_get()()
  iw = InotifyWatch(ed)

  def print_we(wd, m, *a):
    print('E: {!r} {:x} {!r}'.format(wd, m, a))
  iw.process_event = print_we

  for _p in argv[1:]:
    p = _p.encode()
    print('{!r} -> {}'.format(p, iw.add_watch(p, 0xfff)))

  print('Beginning watch.')
  ed.event_loop()


if (__name__ == '__main__'):
  main()
