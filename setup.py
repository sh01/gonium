#!/usr/bin/env python
#Copyright 2007,2008 Sebastian Hagen
# This file is part of gonium.

# gonium is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 
# as published by the Free Software Foundation
#
# gonium is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import distutils.util
import sys
from distutils.core import setup, Extension

if (sys.version_info[0] <= 2):
   raise Exception('This gonium version needs a python >= 3.0') 

ext_modules = [
   Extension('gonium.posix._signal', sources = ['src/posix/_signalmodule.c']),
   Extension('gonium.posix._aio', sources = ['src/posix/_aiomodule.c'], libraries=['rt'])
]

ext_modules_linux = [
   Extension('gonium.linux._io', sources = ['src/linux/_iomodule.c'], libraries=['aio'])
]

platform = distutils.util.get_platform()

if (platform.startswith('linux-')):
   ext_modules += ext_modules_linux
else:
   print('Platform is {0!a}, skipping linux-specific modules.'.format(platform))


setup(name='gonium',
   version='0.6',
   description='Gonium baselib',
   author='Sebastian Hagen',
   author_email='sebastian_hagen@memespace.net',
   url='http://git.memespace.net/git/gonium.git',
   packages=('gonium', 'gonium.fdm', 'gonium.hacks', 'gonium.fdm.ed', 'gonium.posix', 'gonium.linux'),
   ext_modules=ext_modules,
   package_dir={'gonium':'src'}
)

