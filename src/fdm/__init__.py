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

# module history:
# An early predecessor to this module appeared in an ancient gonium version
# (before the project was called that) under the name 'socket_management'.
#
# That module was obsoleted by 'fd_management' in a later gonium version for
# python 2.x.
# Python 3.x brought bytes and bytearrays, which necessitated significant
# changes to fd_management's interface, and suggested more. Since backwards
# compatibility had to be broken a second time in any case, it was decided to
# use this opportunity to completely redesign fd_management's inteface for
# increased clarity and usability.
# As such, this is the third major FD-managing module in gonium's history.
# It's currently in an early stage of implementation, and not yet usable.

from . import ed
from .ed import ED_get
from .ed._base import Timer
from . import stream

