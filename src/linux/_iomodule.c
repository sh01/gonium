#include "Python.h"
#include <unistd.h>
#include <fcntl.h>
#include <libaio.h>

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

/* Linux AIO interface. This requires a Linux >= 2.6.22 or so. */

/* At time of writing (2008-12-22), even the unstable flavor of debian doesn't
   distribute sys/eventfd.h, even though the glibc shared library *does* have
   support. Putting extra demand on distributions or dragging around our own
   copy of that file for 29 bytes of declaration really isn't worth it, we'll
   just define it here.
 */
#define IOCB_FLAG_RESFD 1
extern int eventfd(int, int);

struct timespec tv_zero;

typedef struct {
   PyObject_HEAD
   Py_buffer bufview;
   struct iocb iocb;
   char submitted;
   unsigned long res, res2;
} IORequest;


static PyObject *IORequest_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
   static char *kwlist[] = {"mode", "buf", "filelike", "offset", NULL};
   long long offset;
   int bufflags, fd;
   short mode;
   PyObject *buf, *filelike;
   IORequest *rv;
   
   if (!PyArg_ParseTupleAndKeywords(args, kwargs, "hOOL", kwlist, &mode,
      &buf, &filelike, &offset))
      return NULL;
   
   if (mode == IO_CMD_PREAD) bufflags = PyBUF_WRITABLE;
   else if (mode == IO_CMD_PWRITE) bufflags = PyBUF_SIMPLE;
   else {
      PyErr_SetString(PyExc_ValueError, "Invalid mode.");
      return NULL;
   }
   if ((fd = PyObject_AsFileDescriptor(filelike)) == -1) return NULL;
   if (!(rv = (IORequest*)type->tp_alloc(type, 0))) return NULL;
   rv->bufview.buf = NULL;
   
   if (PyObject_GetBuffer(buf, &rv->bufview, bufflags)) {
      Py_DECREF(rv);
      return NULL;
   }
   
   rv->iocb.aio_lio_opcode = mode;
   rv->iocb.aio_fildes = fd;
   rv->iocb.data = rv;
   rv->iocb.aio_reqprio = 0;
   rv->iocb.u.c.buf = rv->bufview.buf;
   rv->iocb.u.c.nbytes = rv->bufview.len;
   rv->iocb.u.c.offset = offset;
   rv->iocb.u.c.flags = IOCB_FLAG_RESFD;
   return (void*) rv;
}

inline static void _IORequest_bufviewdealloc(Py_buffer *bufview) {
   if (!(bufview->buf)) return;
   PyBuffer_Release(bufview);
   bufview->buf = NULL;
}

static void IORequest_dealloc(IORequest *self) {
   _IORequest_bufviewdealloc(&self->bufview);
   Py_TYPE(self)->tp_free(self);
}

static PyObject* IORequest_submitted_get(IORequest *self, void *closure) {
   return PyLong_FromLong(self->submitted);
}
static PyObject* IORequest_fd_get(IORequest *self, void *closure) {
   return PyLong_FromLong(self->iocb.aio_fildes);
}
static PyObject* IORequest_offset_get(IORequest *self, void *closure) {
   return PyLong_FromLong(self->iocb.u.c.offset);
}
static PyObject* IORequest_rc_get(IORequest *self, void *closure) {
   if (!self->res2) return PyLong_FromLong(self->res);
   if (self->res2 < 0) return PyLong_FromLong(self->res);
   
   PyErr_SetString(PyExc_RuntimeError, "Internal error: res2 positive.");
   return NULL;
}
static PyObject* IORequest_mode_get(IORequest *self, void *closure) {
   return PyLong_FromLong(self->iocb.aio_lio_opcode);
}

static PyMethodDef IORequest_methods[] = {
   {NULL}  /* Sentinel */
};

static PyGetSetDef IORequest_getsetters[] = {
   {"fd", (getter)IORequest_fd_get, (setter)NULL, "File descriptor"},
   {"offset", (getter)IORequest_offset_get, (setter)NULL, "File descriptor"},
   {"rc", (getter)IORequest_rc_get, (setter)NULL, "Return code"},
   {"submitted", (getter)IORequest_submitted_get, (setter)NULL,
    "Boolean variable indicating whether this request has been submitted yet"},
   {"mode", (getter)IORequest_mode_get, (setter)NULL, "Mode of access"},
   {NULL}  /* Sentinel */
};


static PyTypeObject IORequestType = {
   PyVarObject_HEAD_INIT(&PyType_Type, 0)
   "_io.IORequest",         /* tp_name */
   sizeof(IORequest),        /* tp_basicsize */
   0,                         /* tp_itemsize */
   (destructor)IORequest_dealloc, /* tp_dealloc */
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
   "Linux AIO read/write request", /* tp_doc */
   0,		              /* tp_traverse */
   0,		              /* tp_clear */
   0,		              /* tp_richcompare */
   0,		              /* tp_weaklistoffset */
   0,		              /* tp_iter */
   0,		              /* tp_iternext */
   IORequest_methods,         /* tp_methods */
   0,                         /* tp_members */
   IORequest_getsetters,      /* tp_getset */
   0,                         /* tp_base */
   0,                         /* tp_dict */
   0,                         /* tp_descr_get */
   0,                         /* tp_descr_set */
   0,                         /* tp_dictoffset */
   0,                         /* tp_init */
   0,                         /* tp_alloc */
   IORequest_new              /* tp_new */
};

typedef struct {
   PyObject_HEAD
   io_context_t ctx;
   struct io_event *events;
   struct iocb **cbs;
   unsigned long nr_events;
   unsigned long pending_events;
   int fd; // fd to use for signaling completion
} IOManager;

static PyObject *IOManager_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
   static char *kwlist[] = {"nr_events", NULL};
   unsigned nr_events;
   IOManager *rv;
   
   if (!PyArg_ParseTupleAndKeywords(args, kwargs, "I", kwlist,
      &nr_events)) return NULL;
   
   if (!(rv = (IOManager*)type->tp_alloc(type, 0))) return NULL;
   
   if ((rv->fd = eventfd(0,0)) < 0) {
      PyErr_SetFromErrno(PyExc_OSError);
      Py_DECREF(rv);
      return NULL;
   }
   if (fcntl(rv->fd, F_SETFL, O_NONBLOCK)) {
      PyErr_SetFromErrno(PyExc_OSError);
      Py_DECREF(rv);
      return NULL;
   };
   
   memset(&rv->ctx, 0, sizeof(io_context_t));
   if (io_setup(nr_events, &rv->ctx)) {
      PyErr_SetFromErrno(PyExc_OSError);
      close(rv->fd);
      Py_DECREF(rv);
      return NULL;
   };
   
   if (!(rv->events = PyMem_Malloc(sizeof(struct io_event)*nr_events))) {
      close(rv->fd);
      io_destroy(rv->ctx);
      Py_DECREF(rv);
      return NULL;
   }
   if (!(rv->cbs = PyMem_Malloc(sizeof(struct iocb*)*nr_events))) {
      close(rv->fd);
      PyMem_Free(rv->events);
      io_destroy(rv->ctx);
      Py_DECREF(rv);
      return NULL;
   }
   
   rv->nr_events = nr_events;
   rv->pending_events = 0;
   return (void*)rv;
}

static inline void *IOM_iocb_cleanup(IOManager *iom, struct iocb **cb_l) {
   struct iocb **cb;
   IORequest *req;
   for (cb = iom->cbs; cb < cb_l; cb++) {
      req = (*cb)->data;
      if (cb < cb_l - 1) req->submitted = 0;
      Py_DECREF(req);
   }
   return NULL;
}

static PyObject *IOManager_submit(IOManager *self, PyObject *args) {
   PyObject *req_s, *iter;
   IORequest *item;
   Py_ssize_t l;
   int rc;
   struct iocb **cb, **cb_l;
   if (!PyArg_ParseTuple(args, "O", &req_s)) return NULL;

   if ((l = PySequence_Size(req_s)) < 0) return NULL;
   if (l > (self->nr_events - self->pending_events)) {
      PyErr_SetString(PyExc_ValueError, "Queue length exceeded.");
      return NULL;
   }
   
   cb = self->cbs;
   cb_l = cb + (self->nr_events - self->pending_events);
   
   if (!(iter = PyObject_GetIter(req_s))) return NULL;
   for (; (item = (IORequest*) PyIter_Next(iter)); cb++) {
      if (!PyObject_IsInstance((PyObject*) item, (PyObject*) &IORequestType)) {
         Py_DECREF(item);
         PyErr_SetString(PyExc_TypeError, "Elements of argument 0 must be of type IORequest.");
         return IOM_iocb_cleanup(self, cb+1);
      }
      if (cb == cb_l) {
         Py_DECREF(item);
         PyErr_SetString(PyExc_ValueError, "Queue length exceeded (secondary check)");
         return IOM_iocb_cleanup(self, cb+1);
      }
      if (item->submitted) {
         Py_DECREF(item);
         PyErr_SetString(PyExc_ValueError, "Element of argument 0 had already been submitted earlier.");
         return IOM_iocb_cleanup(self, cb+1);
      }
      item->submitted = 1;
      item->iocb.u.c.resfd = self->fd;
      *cb = &item->iocb;
   }
   if (PyErr_Occurred()) return IOM_iocb_cleanup(self, cb);
   
   l = cb - self->cbs;
   rc = io_submit(self->ctx, l, self->cbs);
   if (rc < 0) {
      errno = -rc;
      PyErr_SetFromErrno(PyExc_OSError);
      return IOM_iocb_cleanup(self, cb);
   }
   /* Keep one reference to each element read from the iterable, to make sure
      they aren't deallocated while we wait for their IO requests to complete
   */
   self->pending_events += l;
   Py_RETURN_NONE;
}

static PyObject *IOManager_getevents(IOManager *self, PyObject *args) {
   long min_nr;
   PyObject *timeout = NULL, *rv, *ptype, *pval, *ptb;
   IORequest *req;
   int rc, i;
   double timeout_d = 0.0;
   struct timespec tv, *tvp;
   
   if (!PyArg_ParseTuple(args, "lO", &min_nr, &timeout)) return NULL;
   if (min_nr > self->pending_events) {
      PyErr_SetString(PyExc_ValueError, "min_nr too large: insufficient outstanding requests to fulfill.");
      return NULL;
   }
   
   if (timeout == Py_None) tvp = NULL;
   else {
      tvp = &tv;
      timeout_d = PyFloat_AsDouble(timeout);
      if (PyErr_Occurred()) return NULL;
      tv.tv_sec = (long) timeout_d;
      tv.tv_nsec = ((timeout_d - (double)(long) timeout_d) * 1E9);
   }
   
   rc = io_getevents(self->ctx, min_nr, self->nr_events, self->events, tvp);
   if (rc < 0) {
      PyErr_SetFromErrno(PyExc_OSError);
      return NULL;
   }
   
   self->pending_events -= rc;
   if (!(rv = PyTuple_New(rc))) {
      /* Talk about being painted into a corner.*/
      PyErr_Fetch(&ptype, &pval, &ptb);
      for (i = 0; i < rc; i++) Py_DECREF(self->events[i].data);
      PyErr_Restore(ptype, pval, ptb);
      return NULL;
   }
   for (i = 0; i < rc; i++) {
      req = self->events[i].data;
      req->res = self->events[i].res;
      req->res2 = self->events[i].res2;
      PyTuple_SET_ITEM(rv, i, (PyObject*) req);
   }
   return rv;
}

static void IOManager_dealloc(IOManager *self) {
   io_destroy(self->ctx);
   PyMem_Free(self->events);
   PyMem_Free(self->cbs);
   close(self->fd);
   Py_TYPE(self)->tp_free(self);
}

static PyObject* IOManager_fd_get(IOManager *self, void *closure) {
   return PyLong_FromLong(self->fd);
}

static PyGetSetDef IOManager_getsetters[] = {
   {"fd", (getter)IOManager_fd_get, (setter)NULL, "File descriptor"},
   {NULL}  /* Sentinel */
};


static PyMethodDef IOManager_methods[] = {
   {"submit", (PyCFunction)IOManager_submit, METH_VARARGS,
    "submit(requests) -> NoneType \
    io_submit() wrapper: submit list of AIO requests"},
   {"getevents", (PyCFunction)IOManager_getevents, METH_VARARGS,
    "getevents(min_nr, timeout) -> request-sequence \
    io_getevents() wrapper: Wait for AIO events and return all received events"},
   {NULL}  /* Sentinel */
};


static PyTypeObject IOManagerType = {
   PyVarObject_HEAD_INIT(&PyType_Type, 0)
   "_io.IOManager",           /* tp_name */
   sizeof(IOManager),         /* tp_basicsize */
   0,                         /* tp_itemsize */
   (destructor)IOManager_dealloc, /* tp_dealloc */
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
   "Linux AIO-set managing objects",/* tp_doc */
   0,		              /* tp_traverse */
   0,		              /* tp_clear */
   0,		              /* tp_richcompare */
   0,		              /* tp_weaklistoffset */
   0,		              /* tp_iter */
   0,		              /* tp_iternext */
   IOManager_methods,         /* tp_methods */
   0,                         /* tp_members */
   IOManager_getsetters,      /* tp_getset */
   0,                         /* tp_base */
   0,                         /* tp_dict */
   0,                         /* tp_descr_get */
   0,                         /* tp_descr_set */
   0,                         /* tp_dictoffset */
   0,                         /* tp_init */
   0,                         /* tp_alloc */
   IOManager_new              /* tp_new */
};


static struct PyModuleDef _iomodule = {
   PyModuleDef_HEAD_INIT,
   "_io",
   NULL,
   -1
};


PyMODINIT_FUNC PyInit__io(void) {
   PyObject *m = PyModule_Create(&_iomodule);
   if (!m) return NULL;
   tv_zero.tv_sec = 0;
   tv_zero.tv_nsec = 0;
   
   /* IORequest type setup */
   if (PyType_Ready(&IORequestType) < 0) return NULL;
   Py_INCREF(&IORequestType);
   PyModule_AddObject(m, "IORequest", (PyObject *)&IORequestType);

   /* IOManager type setup */
   if (PyType_Ready(&IOManagerType) < 0) return NULL;
   Py_INCREF(&IOManagerType);
   PyModule_AddObject(m, "IOManager", (PyObject *)&IOManagerType);

   /* libaio.h constants */
   PyModule_AddObject(m, "IO_CMD_PREAD", PyLong_FromLong(IO_CMD_PREAD));
   PyModule_AddObject(m, "IO_CMD_PWRITE", PyLong_FromLong(IO_CMD_PWRITE));
   PyModule_AddObject(m, "IO_CMD_FSYNC", PyLong_FromLong(IO_CMD_FSYNC));
   PyModule_AddObject(m, "IO_CMD_FDSYNC", PyLong_FromLong(IO_CMD_FDSYNC));
   PyModule_AddObject(m, "IO_CMD_POLL", PyLong_FromLong(IO_CMD_POLL));
   PyModule_AddObject(m, "IO_CMD_NOOP", PyLong_FromLong(IO_CMD_NOOP));
   return m;
}
