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
from distutils.core import setup

if (sys.version_info[0] <= 2):
   raise Exception('This gonium version needs a python >= 3.0') 

setup(name='gonium',
   version='0.6',
   description='Gonium baselib',
   author='Sebastian Hagen',
   author_email='sebastian_hagen@memespace.net',
   url='http://git.memespace.net/git/gonium.git',
   packages=('gonium', 'gonium.fd_management', 'gonium.posix', 'gonium.linux'),
   package_dir={'gonium':'src'}
)

