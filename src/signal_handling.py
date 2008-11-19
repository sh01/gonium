#!/usr/bin/env python
# Copyright Sebastian Hagen 2006, 2007
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

import signal
import os
import sys
import logging
import thread

import socket_management
try:
   import data_pickling
except ImportError:
   data_pickling = None

logger = logging.getLogger('signal_handling')

def program_shutdown(signal, stack_frame):
   logger.log(50, 'Caught signal %s, shutting down.' % (signal,))
   del(stack_frame)
   try:
      # We can't do this safely directly, since the Timer structures might be
      # in active use by a the code interrupted by this signal handler.
      # Using a thread is somewhat ugly, but it won't live very long anyway.
      thread.start_new_thread(socket_management.Timer.shutdown_start, ())
   except:
      logger.log(40, 'Error shutting down cleanly; doing it by force. Error was:', exc_info=True)
      timer_set = False
   else:
      timer_set = True

   if (data_pickling):
      data_pickling.save_variables()
   if not (timer_set):
      os._exit(signal)


signal.signal(signal.SIGTERM, program_shutdown)
signal.signal(signal.SIGINT, program_shutdown)
