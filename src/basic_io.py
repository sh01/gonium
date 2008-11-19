#!/usr/bin/env python
#Copyright 2004 Sebastian Hagen
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
import os.path
import time
import logging
import logging.handlers


settings = {}
execfile('settings.py')

log_settings = {
   #Entries should be of the form (<loggername_list>, <handlerclass>, [, [handler_args][, handler_kwargs[, calls]]])
   'handlers':[],
   'handler_defaults': {
      logging.handlers.RotatingFileHandler:{
         'kwargs':{
            'maxBytes':1048576,
            'backupCount':9,
            },
         'calls':(('setLevel', (logging.DEBUG,)),)
         }
      }
   }

if ('basic_io' in settings):
   log_settings.update(settings['basic_io'])


logger = logging.getLogger()
logger.setLevel(0)

formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')

def log_init():
   for handler_entry in log_settings['handlers']:
      if (len(handler_entry) < 2):
         raise ValueError('handler_entry %s has too few elements (expected at least two: loggername and handlerclass)' % (handler_entry,))
      
      handler_class = handler_entry[1]
      
      if ((handler_class in log_settings['handler_defaults']) and ('kwargs' in log_settings['handler_defaults'][handler_class])):
         handler_kwargs = log_settings['handler_defaults'][handler_class]['kwargs'].copy()
      else:
         handler_kwargs = {}
      
      if (len(handler_entry) >= 3):
         handler_args = handler_entry[2]
         if (len(handler_entry) >= 4):
            handler_kwargs.update(handler_entry[3])
      elif ((handler_class in log_settings['handler_defaults']) and ('args' in log_settings['handler_defaults'][handler_class])):
         handler_args = log_settings['handler_defaults'][handler_class]['args'][:]
      else:
         handler_args = ()

      if (handler_class in (logging.handlers.RotatingFileHandler, logging.FileHandler)):
         if (len(handler_args) > 0):
            target_filename = handler_args[0]
         elif ('filename' in handler_kwargs):
            target_filename = handler_kwargs['filename']
         else:
            target_filename = None
            
         if (target_filename and (type(target_filename) == str)):
            target_dirname = os.path.dirname(target_filename)
            if (target_dirname and (not os.path.exists(target_dirname))):
               os.makedirs(target_dirname, 0755)
      
      handler = handler_class(*handler_args, **handler_kwargs)
      handler.setFormatter(formatter)
         
      if (len(handler_entry) >= 5):
         calls = handler_entry[4]
      elif ((handler_class in log_settings['handler_defaults']) and ('calls' in log_settings['handler_defaults'][handler_class])):
         calls = log_settings['handler_defaults'][handler_class]['calls']
      else:
         calls = ()
         
      for call_entry in calls:
         if (len(call_entry) < 1):
            raise ValueError('Invalid call-entry %s (expected at least one element).' % (call_entry,))

         if (len(call_entry) >= 2):
            call_args = call_entry[1]
            if (len(call_entry) >= 3):
               call_kwargs = call_entry[2]
            else:
               call_kwargs = {}
         else:
            call_args = ()
            call_kwargs = {}

         getattr(handler, call_entry[0])(*call_args, **call_kwargs)
         
      for loggername in handler_entry[0]:
         logging.getLogger(loggername).addHandler(handler)

         