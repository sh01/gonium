#include "Python.h"
#include <aio.h>
#include <signal.h>

/*
 * Copyright 2008 Sebastian Hagen
 *  This file is part of gonium.
 * 
 *  gonium is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 2 of the License, or
 *  (at your option) any later version.
 * 
 *  gonium is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/* POSIX AIO interface
   TODO: actually implement this
*/

/* Give productive feedback to people trying to link this with Python 2.x */
#if PY_MAJOR_VERSION < 3
#error This file requires python >= 3.0
#endif


static struct PyModuleDef _aiomodule = {
   PyModuleDef_HEAD_INIT,
   "_aio",
   NULL,
   -1//,
   //module_methods
};


PyMODINIT_FUNC
PyInit__aio(void) {
   PyObject *m = PyModule_Create(&_aiomodule);
   if (!m) return NULL;
   
   /* aio.h posix constants */
   PyModule_AddObject(m, "AIO_ALLDONE", PyLong_FromLong(AIO_ALLDONE));
   PyModule_AddObject(m, "AIO_CANCELED", PyLong_FromLong(AIO_CANCELED));
   PyModule_AddObject(m, "AIO_NOTCANCELED", PyLong_FromLong(AIO_NOTCANCELED));
   PyModule_AddObject(m, "LIO_NOP", PyLong_FromLong(LIO_NOP));
   PyModule_AddObject(m, "LIO_NOWAIT", PyLong_FromLong(LIO_NOWAIT));
   PyModule_AddObject(m, "LIO_READ", PyLong_FromLong(LIO_READ));
   PyModule_AddObject(m, "LIO_WAIT", PyLong_FromLong(LIO_WAIT));
   PyModule_AddObject(m, "LIO_WRITE", PyLong_FromLong(LIO_WRITE));
   
   /* signal.h posix constants */
   PyModule_AddObject(m, "SIGEV_NONE", PyLong_FromLong(SIGEV_NONE));
   PyModule_AddObject(m, "SIGEV_SIGNAL", PyLong_FromLong(SIGEV_SIGNAL));
   PyModule_AddObject(m, "SIGEV_THREAD", PyLong_FromLong(SIGEV_THREAD));
   
   return m;
}
