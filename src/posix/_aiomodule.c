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

/* POSIX AIO interface */

/* Give productive feedback to people trying to link this with Python 2.x */
#if PY_MAJOR_VERSION < 3
#error This file requires python >= 3.0
#endif

#define CBPA_LEN_START 32

#ifdef HAVE_LARGEFILE_SUPPORT
typedef off64_t _fdoff_t;
#define __AIORN_FMT "iOOL"
#else
typedef off_t _fdoff_t;
#define __AIORN_FMT "iOOl"
#endif

struct timespec tv_zero;

typedef struct {
   PyObject_HEAD
   Py_buffer bufview;
   int mode;
   int fd;
   _fdoff_t offset;
   char submitted;
   ssize_t rc; //return code
} AIORequest;

static PyObject *AIORequest_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
   static char *kwlist[] = {"mode", "buf", "filelike", "offset", NULL};
   int mode, bufflags, fd;
   PyObject *buf, *filelike;
   AIORequest *rv;
   _fdoff_t offset;
   
   if (!PyArg_ParseTupleAndKeywords(args, kwargs, __AIORN_FMT, kwlist, &mode,
      &buf, &filelike, &offset))
      return NULL;
   
   if (mode == LIO_READ) bufflags = PyBUF_WRITABLE;
   else if (mode == LIO_WRITE) bufflags = PyBUF_SIMPLE;
   else {
      PyErr_SetString(PyExc_ValueError, "Invalid mode.");
      return NULL;
   }
   if ((fd = PyObject_AsFileDescriptor(filelike)) == -1) return NULL;
   if (!(rv = (AIORequest*)type->tp_alloc(type, 0))) return NULL;
   rv->bufview.buf = NULL;
   
   if (PyObject_GetBuffer(buf, &rv->bufview, bufflags)) {
      Py_DECREF(rv);
      return NULL;
   }
   rv->fd = fd;
   rv->offset = offset;
   rv->mode = mode;
   rv->submitted = 0;
   rv->rc = 0;
   return (void*) rv;
}

inline static void _AIORequest_bufviewdealloc(Py_buffer *bufview) {
   if (!(bufview->buf)) return;
   PyBuffer_Release(bufview);
   bufview->buf = NULL;
}

static void AIORequest_dealloc(AIORequest *self) {
   _AIORequest_bufviewdealloc(&self->bufview);
   Py_TYPE(self)->tp_free(self);
}

static PyObject* AIORequest_fd_get(AIORequest *self, void *closure) {
   return PyLong_FromLong(self->fd);
}
static PyObject* AIORequest_mode_get(AIORequest *self, void *closure) {
   return PyLong_FromLong(self->mode);
}
static PyObject* AIORequest_offset_get(AIORequest *self, void *closure) {
   return PyLong_FromLongLong(self->offset);
}
static PyObject* AIORequest_rc_get(AIORequest *self, void *closure) {
   return PyLong_FromLong(self->rc);
}
static PyObject* AIORequest_submitted_get(AIORequest *self, void *closure) {
   return PyLong_FromLong(self->submitted);
}

static PyMethodDef AIORequest_methods[] = {
   {NULL}  /* Sentinel */
};

static PyGetSetDef AIORequest_getsetters[] = {
   {"fd", (getter)AIORequest_fd_get, (setter)NULL, "File descriptor"},
   {"offset", (getter)AIORequest_offset_get, (setter)NULL, "File descriptor"},
   {"rc", (getter)AIORequest_rc_get, (setter)NULL, "Return code"},
   {"submitted", (getter)AIORequest_submitted_get, (setter)NULL,
    "Boolean variable indicating whether this request has been submitted yet"},
   {"mode", (getter)AIORequest_mode_get, (setter)NULL, "Mode of access"},
   {NULL}  /* Sentinel */
};


static PyTypeObject AIORequestType = {
   PyVarObject_HEAD_INIT(&PyType_Type, 0)
   "_aio.AIORequest",         /* tp_name */
   sizeof(AIORequest),        /* tp_basicsize */
   0,                         /* tp_itemsize */
   (destructor)AIORequest_dealloc, /* tp_dealloc */
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
   Py_TPFLAGS_BASETYPE,       /* tp_flags */
   "AIO read/write request",  /* tp_doc */
   0,		              /* tp_traverse */
   0,		              /* tp_clear */
   0,		              /* tp_richcompare */
   0,		              /* tp_weaklistoffset */
   0,		              /* tp_iter */
   0,		              /* tp_iternext */
   AIORequest_methods,        /* tp_methods */
   0,                         /* tp_members */
   AIORequest_getsetters,     /* tp_getset */
   0,                         /* tp_base */
   0,                         /* tp_dict */
   0,                         /* tp_descr_get */
   0,                         /* tp_descr_set */
   0,                         /* tp_dictoffset */
   0,                         /* tp_init */
   0,                         /* tp_alloc */
   AIORequest_new             /* tp_new */
};


typedef struct {
   PyObject_HEAD
   struct aiocb **cbpa;     //control block pointer array
   AIORequest **rpa; //request pointer array
   size_t cbpa_len;
   struct sigevent se;
} AIOManager;


static PyObject *AIOManager_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
   AIOManager *rv;
   if (!(rv = (AIOManager*)type->tp_alloc(type, 0))) return NULL;
   rv->cbpa = NULL;
   rv->cbpa_len = 0;
   rv->se.sigev_notify = SIGEV_SIGNAL;
   rv->se.sigev_signo = SIGIO;
   rv->se.sigev_value.sival_int = 0;
   
   return (void*) rv;
}

static inline int AIOManager_aiocbrs(AIOManager *self, size_t elcount_new) {
   size_t i;
   struct aiocb **cbpa_new;
   AIORequest **rpa_new;
   
   if (!(cbpa_new = PyMem_Malloc(sizeof(struct aiocb*)*elcount_new))) return -1;
   if (!(rpa_new = PyMem_Malloc(sizeof(struct AIORequest*)*elcount_new))) {
      PyMem_Free(cbpa_new);
      return -1;
   }
   
   for (i = 0; i < self->cbpa_len; i++) {
      cbpa_new[i] = self->cbpa[i];
      rpa_new[i] = self->rpa[i];
   }
   for (i = self->cbpa_len; i < elcount_new; i++) {
      cbpa_new[i] = NULL;
      rpa_new[i] = NULL;
   }
   
   // This might fail horribly if we're called reentrantly (e.g. from a signal
   // handler); can this happen in CPython?
   PyMem_Free(self->cbpa);
   PyMem_Free(self->rpa);
   self->cbpa = cbpa_new;
   self->rpa = rpa_new;
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
   int rc;
   PyObject *rv;
   AIORequest *req;
   struct aiocb *cb;
   size_t i;
   
   if (!PyArg_ParseTuple(args, "d", &timeout)) return NULL;
   tv.tv_sec = (long) timeout;
   tv.tv_nsec = ((timeout - (double)(long) timeout) * 1E9);
   
   rc = aio_suspend((const struct aiocb **)self->cbpa, self->cbpa_len, &tv);
   if (rc) {
      if (errno == EAGAIN) return PyList_New(0);
      PyErr_SetFromErrno(PyExc_IOError);
      return NULL;
   }
   rv = PyList_New(0);
   
   for (i = 0; i < self->cbpa_len; i++) {
      if (!(cb = self->cbpa[i])) continue;
      if ((rc = aio_error(cb)) == EINPROGRESS) continue;
      if (!rc) rc = aio_return(self->cbpa[i]);
      req = self->rpa[i];
      req->rc = rc;
      if (PyList_Append(rv, (PyObject*) req)) {
         Py_DECREF(rv);
         return NULL;
      }
      Py_DECREF(req);
      PyMem_Free(self->cbpa[i]);
      self->cbpa[i] = NULL;
      self->rpa[i] = NULL;
   }
   return rv;
}

static PyObject *AIOManager_get_results(AIOManager *self, PyObject *args) {
   PyObject *eventlist, *pyevent, *rv;
   AIORequest *req;
   Py_ssize_t evcount, i, event;
   struct aiocb *cb;
   int rc;
   
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
      if ((!(cb = self->cbpa[i])) || ((rc = aio_error(cb)) == EINPROGRESS)) {
         Py_INCREF(Py_None);
         if (PyTuple_SetItem(rv, i, Py_None)) goto error_exit;
         continue;
      }
      req = self->rpa[i];
      if (!rc) rc = aio_return(self->cbpa[i]);
      req->rc = rc;
      _AIORequest_bufviewdealloc(&req->bufview);
      
      if (PyTuple_SetItem(rv, i, (PyObject*) req)) goto error_exit;
      
      PyMem_Free(self->cbpa[i]);
      self->cbpa[i] = NULL;
      self->rpa[i] = NULL;
   }
   
   return rv;
   error_exit:
   Py_DECREF(rv);
   return NULL;
}

static PyObject *AIOManager_io(AIOManager *self, PyObject *args) {
   AIORequest *req;
   size_t i;
   struct aiocb *cb_new;
   int rc;
   
   if (!PyArg_ParseTuple(args, "O", &req)) return NULL;
   if (!PyObject_IsInstance((PyObject*) req, (PyObject*) &AIORequestType)) {
      PyErr_SetString(PyExc_TypeError, "Argument 0 must be of type AIORequest.");
      return NULL;
   }
   if (req->submitted) {
      PyErr_SetString(PyExc_TypeError, "AIORequest has already been submitted earlier.");
      return NULL;
   }
   req->submitted = 1;
   
   i = 0;
   while ((i < self->cbpa_len) && (self->rpa[i])) i++;
   // resize arrays
   if ((i == self->cbpa_len) && (AIOManager_aiocbrs(self, self->cbpa_len*2)))
      return NULL;
   
   if (!(cb_new = PyMem_Malloc(sizeof(struct aiocb)))) return NULL;
   
   cb_new->aio_fildes = req->fd;
   cb_new->aio_reqprio = 0;
   cb_new->aio_buf = req->bufview.buf;
   cb_new->aio_nbytes = req->bufview.len;
   cb_new->aio_offset = req->offset;
   cb_new->aio_sigevent = self->se;
   cb_new->aio_sigevent.sigev_value.sival_int = i;
   
   self->cbpa[i] = cb_new;
   self->rpa[i] = req;
   if (req->mode == LIO_READ) rc = aio_read(cb_new);
   else if (req-> mode == LIO_WRITE) rc = aio_write(cb_new);
   else {
      // can't happen
      self->cbpa[i] = NULL;
      self->rpa[i] = NULL;
      PyMem_Free(cb_new);
      PyErr_SetString(PyExc_SystemError, "Can't happen event (_aio bug?): AIORequest had bogus type");
      return NULL;
   }
   if (rc) {
      self->cbpa[i] = NULL;
      self->rpa[i] = NULL;
      PyMem_Free(cb_new);
      PyErr_SetFromErrno(PyExc_IOError);
      return NULL;
   }
   
   Py_INCREF(req);
   Py_RETURN_NONE;
}

static PyMethodDef AIOManager_methods[] = {
   {"io", (PyCFunction)AIOManager_io, METH_VARARGS, "aio_{read,write} wrapper: submit AIO request"},
   {"suspend", (PyCFunction)AIOManager_suspend, METH_VARARGS, "aio_suspend() wrapper: Wait for AIO event and return all received events"},
   {"get_results", (PyCFunction)AIOManager_get_results, METH_VARARGS, "Call aio_error,aio_return on specified events"},
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
   Py_TPFLAGS_BASETYPE,       /* tp_flags */
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
   
   /* AIORequest type setup */
   if (PyType_Ready(&AIORequestType) < 0) return NULL;
   Py_INCREF(&AIORequestType);
   PyModule_AddObject(m, "AIORequest", (PyObject *)&AIORequestType);
   
   /* AIOManager type setup */
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
