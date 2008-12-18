#include "Python.h"
#include <aio.h>
#include <signal.h>
#include <time.h>

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

#define CBPA_LEN_START 32

struct timespec tv_zero;

typedef struct {
   PyObject_HEAD
   struct aiocb **cbpa;
   size_t cbpa_len;
} AIOManager;


static PyObject *AIOManager_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
   AIOManager *rv;
   if (!(rv = (AIOManager*)type->tp_alloc(type, 0))) return NULL;
   rv->cbpa = NULL;
   rv->cbpa_len = 0;
   return (void*) rv;
}

int AIOManager_aiocbrs(AIOManager *self, size_t elcount_new) {
   size_t i;
   struct aiocb **cbpa_new;
   
   if (!(cbpa_new = PyMem_Malloc(sizeof(struct aiocb*)*elcount_new))) return -1;
   for (i = 0; i < elcount_new; i++)
      cbpa_new[i] = ((i < self->cbpa_len) ? self->cbpa[i] : NULL);
   
   PyMem_Free(self->cbpa);
   self->cbpa = cbpa_new;
   self->cbpa_len = elcount_new;
   return 0;
}

static PyObject *AIOManager_init(AIOManager *self, PyObject *args, PyObject *kwargs) {
   static char *kwlist[] = {"length", NULL};
   Py_ssize_t elcount = CBPA_LEN_START;
   if (!PyArg_ParseTupleAndKeywords(args, kwargs,"|n", kwlist, &elcount)) return NULL;
   self->cbpa = NULL;
   self->cbpa_len = 0;
   if (AIOManager_aiocbrs(self, elcount)) return NULL;
   Py_RETURN_NONE;
}

static PyObject *AIOManager_suspend(AIOManager *self, PyObject *args) {
   struct timespec tv;
   double timeout = 0.0;
   if (!PyArg_ParseTuple(args, "d", &timeout)) return NULL;
   tv.tv_sec = (long) timeout;
   tv.tv_nsec = ((timeout - (double)(long) timeout) * 1E9);
   if (!aio_suspend((const struct aiocb **)self->cbpa, self->cbpa_len, &tv)) Py_RETURN_NONE;
   PyErr_SetFromErrno(PyExc_Exception);
   return NULL;
}

static PyObject *AIOManager_events_process(AIOManager *self, PyObject *args) {
   PyObject *eventlist, *pyevent, *rv;
   Py_ssize_t evcount, i, event;
   struct aiocb *cb;
   
   if (!PyArg_ParseTuple(args, "O", &eventlist)) return NULL;
   evcount = PySequence_Size(eventlist);
   if (evcount == -1) return NULL;
   if (!(rv = PyTuple_New(evcount))) return NULL;
   
   for (i = 0; i < evcount; i++) {
      pyevent = PySequence_GetItem(eventlist, i);
      if (((event = PyLong_AsSize_t(pyevent)) == -1) && PyErr_Occurred()) goto error_exit;
      if (event >= self->cbpa_len) {
          PyErr_SetString(PyExc_ValueError, "Excessively large index.");
          goto error_exit;
      }
      if (!(cb = self->cbpa[i])) {
         Py_INCREF(Py_None);
         if (PyTuple_SetItem(rv, i, Py_None)) goto error_exit;
         continue;
      }
      if (!(pyevent = PyLong_FromLong(aio_error(cb)))) return NULL;
      if (PyTuple_SetItem(rv, i, pyevent)) goto error_exit;
      continue;
      
      error_exit:
      Py_DECREF(rv);
      return NULL;
   }
   return rv;
}


static PyMethodDef AIOManager_methods[] = {
   {"suspend", (PyCFunction)AIOManager_suspend, METH_VARARGS, "aio_suspend() wrapper: Wait for AIO event"},
   {"events_process", (PyCFunction)AIOManager_events_process, METH_VARARGS, "Call aio_error on specified events"},
   {NULL}  /* Sentinel */
};


static PyTypeObject AIOManagerType = {
   PyVarObject_HEAD_INIT(&PyType_Type, 0)
   "_aio.AIOManager",         /* tp_name */
   sizeof(AIOManager),        /* tp_basicsize */
   0,                         /* tp_itemsize */
   0,                         /* tp_dealloc */
   0,                         /* tp_print */
   0,                         /* tp_getattr */
   0,                         /* tp_setattr */
   0,                         /* tp_compare */
   0,                         /* tp_repr */
   0,                         /* tp_as_number */
   0,                         /* tp_as_sequence */
   0,                         /* tp_as_mapping */
   0,                         /* tp_hash  */
   0,                         /* tp_call */
   0,                         /* tp_str */
   0,                         /* tp_getattro */
   0,                         /* tp_setattro */
   0,                         /* tp_as_buffer */
   Py_TPFLAGS_DEFAULT,        /* tp_flags */
   "AIO-set managing objects",/* tp_doc */
   0,		              /* tp_traverse */
   0,		              /* tp_clear */
   0,		              /* tp_richcompare */
   0,		              /* tp_weaklistoffset */
   0,		              /* tp_iter */
   0,		              /* tp_iternext */
   AIOManager_methods,        /* tp_methods */
   0,                         /* tp_members */
   0,                         /* tp_getset */
   0,                         /* tp_base */
   0,                         /* tp_dict */
   0,                         /* tp_descr_get */
   0,                         /* tp_descr_set */
   0,                         /* tp_dictoffset */
   (initproc)AIOManager_init, /* tp_init */
   0,                         /* tp_alloc */
   AIOManager_new             /* tp_new */
};


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
   tv_zero.tv_sec = 0;
   tv_zero.tv_nsec = 0;
   
   /* SigInfo type setup */
   if (PyType_Ready(&AIOManagerType) < 0) return NULL;
   Py_INCREF(&AIOManagerType);
   PyModule_AddObject(m, "AIOManager", (PyObject *)&AIOManagerType);
   
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
