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

# Reusable code for gonium-internal debugging. *Not* in any way a public API.

import logging

sformatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def streamlogger_setup(ll_out=20, stream=None):
   logger = logging.getLogger()
   logger.setLevel(0)
   handler = logging.StreamHandler(stream)
   handler.setLevel(ll_out)
   handler.setFormatter(sformatter)
   logger.addHandler(handler)
