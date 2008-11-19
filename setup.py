#!/usr/bin/env python
#Copyright 2007 Sebastian Hagen
# This file is part of gonium.

# gonium is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 
# as published by the Free Software Foundation

# gonium is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with gonium; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

from distutils.core import setup

setup(name='gonium',
   version='0.5',
   description='Gonium baselib',
   author='Sebastian Hagen',
   author_email='sebastian_hagen@memespace.net',
   url='svn://svn.memespace.net/hobby/gonium',
   packages=('gonium','gonium.fd_management'),
   package_dir={'gonium':'src'}
)

