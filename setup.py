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

import sys
from distutils.core import setup, Extension

if (sys.version_info[0] <= 2):
   raise Exception('This gonium version needs a python >= 3.0') 

module1 = Extension('gonium.posix._signal', sources = ['src/posix/_signalmodule.c'])


setup(name='gonium',
   version='0.6',
   description='Gonium baselib',
   author='Sebastian Hagen',
   author_email='sebastian_hagen@memespace.net',
   url='http://git.memespace.net/git/gonium.git',
   packages=('gonium', 'gonium.fdm', 'gonium.posix', 'gonium.linux'),
   ext_modules = [module1],
   package_dir={'gonium':'src'}
)

