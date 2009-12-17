/*
 * Copyright 2009 Sebastian Hagen
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

/* POSIX fd2fd interface */

#include "Python.h"
#include <time.h>

#include <unistd.h>
#include <pthread.h>

/* _GNU_SOURCE is set by Python.h */
#include <fcntl.h>

/* Give productive feedback to people trying to link this with Python 2.x */
#if PY_MAJOR_VERSION < 3
#error This file requires python >= 3.0
#endif

#define SRC_ISMEM 1
#define DST_ISMEM 2

static char scratch_buf[10240];

#ifdef __USE_GNU
  typedef loff_t lt_off;
#else
  typedef off_t lt_off;
#endif

static PyTypeObject DataTransferDispatcherType;
typedef struct __DataTransferDispatcher DataTransferDispatcher;
typedef struct __DataTransferRequest DataTransferRequest;
/* Not intended for dereferencing. Using local scratch buf because it's
   guaranteed to be distinct from all possible real atr pointers. */
DataTransferRequest *dtr_unqueued = (DataTransferRequest*) &scratch_buf;

typedef struct __t_wt_data {
   struct __DataTransferDispatcher *dtd;
   pthread_t thread;
   int active;
   #ifdef __USE_GNU
   int pfd[2];
   #endif
} t_wt_data;

struct __DataTransferRequest {
   PyObject_HEAD
   DataTransferDispatcher *dtd;
   DataTransferRequest *next;
   PyObject *py_src, *py_dst, *opaque;
   
   int ttype;
   union {
      struct {
         int fd;
         lt_off off;
         int use_off;
      } fl;
      
      Py_buffer mem;
   } src, dst;
   size_t l;
};


void static inline cd_fd2mem(DataTransferRequest *dtr) {
   ssize_t e;
   if (dtr->src.fl.use_off)
      e = pread(dtr->src.fl.fd, dtr->dst.mem.buf, dtr->l, dtr->src.fl.off);
   else
      e = read(dtr->src.fl.fd, dtr->dst.mem.buf, dtr->l);
   if (e != dtr->l) abort();
}

void static inline cd_mem2mem(DataTransferRequest *dtr) {
   memmove(dtr->dst.mem.buf, dtr->src.mem.buf, dtr->l);
}

#ifdef __USE_GNU
#include <sys/uio.h>
#define SET_SRCOFF if (dtr->src.fl.use_off) { src_off = dtr->src.fl.off; p_src_off = &src_off; } else p_src_off = NULL;
#define SET_DSTOFF if (dtr->dst.fl.use_off) { dst_off = dtr->dst.fl.off; p_dst_off = &dst_off; } else p_dst_off = NULL;

void static copy_data(DataTransferRequest *dtr, t_wt_data *wt_data) {
   long e, f;
   size_t l;
   unsigned int dflags = SPLICE_F_MOVE;
   lt_off src_off, dst_off, *p_src_off, *p_dst_off;
   struct iovec iv;
   
   switch (dtr->ttype) {
      case 0: /* fd2fd*/
         SET_SRCOFF;
         SET_DSTOFF;
         l = dtr->l;
         
         while (l) {
            e = splice(dtr->src.fl.fd, p_src_off, wt_data->pfd[1], NULL, l,
               SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
            if (e <= 0) abort();
            l -= e;
            f = splice(wt_data->pfd[0], NULL, dtr->dst.fl.fd, p_dst_off, e,
               dflags | l ? SPLICE_F_MORE : 0);
            if (e != f) abort();
         }
         break;
      
      case SRC_ISMEM: /* mem2fd */
         SET_DSTOFF;
         iv.iov_base = dtr->src.mem.buf;
         iv.iov_len = dtr->l;
         while (iv.iov_len > 0) {
            e = vmsplice(wt_data->pfd[1], &iv, 1, 0);
            if (e <= 0) abort();
            iv.iov_len -= e;
            f = splice(wt_data->pfd[0], NULL, dtr->dst.fl.fd, p_dst_off,
               e, dflags | iv.iov_len ? SPLICE_F_MORE : 0);
            
            if (e != f) abort();
            iv.iov_base = (((char*) iv.iov_base) + e);
         }
         break;
      
      case DST_ISMEM: /* fd2mem */
         cd_fd2mem(dtr);
         break;
      
      case DST_ISMEM | SRC_ISMEM: /* mem2mem */
         cd_mem2mem(dtr);
         break;
      
      default:
         abort();
   }
}

#else
#define IOBUFSIZE (1024*1024)
void static copy_data(DataTransferRequest *dtr, t_wt_data *wt_data) {
   ssize_t e,f;
   size_t l;
   char *buf;
   lt_off src_off, dst_off;
   
   switch (dtr->ttype) {
      case 0: /* fd2fd*/
         buf = malloc(IOBUFSIZE);
         if (!buf) abort();
         l = dtr->l;
         if (dtr->src.fl.use_off) src_off = dtr->src.fl.off;
         if (dtr->dst.fl.use_off) dst_off = dtr->dst.fl.off;
         while (l) {
            if (dtr->src.fl.use_off) {
               e = pread(dtr->src.fl.fd, buf, (l > IOBUFSIZE) ? IOBUFSIZE : l, src_off);
               if (e <= 0) abort();
               src_off += e;
            } else {
               e = read(dtr->src.fl.fd, buf, (l > IOBUFSIZE) ? IOBUFSIZE : l);
               if (e <= 0) abort();
            }
            
            if (dtr->dst.fl.use_off) {
               f = pwrite(dtr->dst.fl.fd, buf, e, dst_off);
               if (f != e) abort();
               dst_off += f;
            } else {
               f = write(dtr->dst.fl.fd, buf, e);
               if (f != e) abort();
            }
            l -= e;
         }
         free(buf);
         break;
         
      case SRC_ISMEM: /* mem2fd */
         if (dtr->dst.fl.use_off)
            e = pwrite(dtr->dst.fl.fd, dtr->src.mem.buf, dtr->l, dtr->dst.fl.off);
         else
            e = write(dtr->dst.fl.fd, dtr->src.mem.buf, dtr->l);
         if (e <= 0) abort();
         break;
      
      case DST_ISMEM: /* fd2mem */
         cd_fd2mem(dtr);
         break;
      
      case DST_ISMEM | SRC_ISMEM: /* mem2mem */
         cd_mem2mem(dtr);
         break;
      
      default:
         abort();
   }
}
#endif

struct __DataTransferDispatcher {
   PyObject_HEAD
   pthread_mutex_t reqs_mtx, res_mtx;
   pthread_cond_t reqs_cond;
   t_wt_data *wt_data;             /* worker thread data */
   long wtcount;                   /* worker thread count */
   DataTransferRequest *req, *res; /* requests, results */
   DataTransferRequest **req_tp;   /* request tail pointer */
   size_t reqcount;                /* request count */
   size_t rescount;                /* result count */
   int spfd[2];                    /* signal pipe */
};


static DataTransferRequest* DataTransferRequest_new(PyTypeObject *type,
      PyObject *args, PyObject *kwargs) {
   
   DataTransferRequest *self;
   static char *kwlist[] = {"dtd", "src", "dst", "off_in", "off_out",
      "length", "opaque", NULL};
   PyObject *py_src, *py_dst, *poff_in, *poff_out, *opaque;
   DataTransferDispatcher *dtd;
   long long off, len;
   
   if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!OOOOLO", kwlist,
      &DataTransferDispatcherType, &dtd,
      &py_src, &py_dst, &poff_in, &poff_out, &len, &opaque)) return NULL;
   
   self = (DataTransferRequest*) type->tp_alloc(type, 0);
   if (!self) return self;
   
   self->next = dtr_unqueued;
   self->ttype = 0;
   Py_INCREF(py_src);
   self->py_src = py_src;
   Py_INCREF(py_dst);
   self->py_dst = py_dst;
   Py_INCREF(dtd);
   self->dtd = dtd;
   Py_INCREF(opaque);
   self->opaque = opaque;
   
   /* XXX: Potential for silent overflows here if lt_off is 32bit. Mitigate that somehow? */
   if ((self->src.fl.fd = PyObject_AsFileDescriptor(py_src)) < 0) {
      PyErr_Clear();
      if (PyObject_GetBuffer(py_src, &self->src.mem, PyBUF_SIMPLE)) goto fail;
      self->ttype |= SRC_ISMEM;
      if (self->src.mem.len < len) {
         PyErr_SetString(PyExc_ValueError, "src memory object too short.");
         goto fail;
      }
   } else {
      if (poff_in == Py_None) {
         self->src.fl.use_off = 0;
      } else {
         if ((off = PyLong_AsLongLong(poff_in)) < 0) goto fail;
         self->src.fl.off = off;
         self->src.fl.use_off = 1;
      }
   }
   
   if ((self->dst.fl.fd = PyObject_AsFileDescriptor(py_dst)) < 0) {
      PyErr_Clear();
      if (PyObject_GetBuffer(py_dst, &self->dst.mem, PyBUF_WRITABLE)) goto fail;
      self->ttype |= DST_ISMEM;
      if (self->dst.mem.len < len) {
         PyErr_SetString(PyExc_ValueError, "dst memory object too short.");
         goto fail;
      }
   } else {
      if (poff_out == Py_None) {
         self->dst.fl.use_off = 0;
      } else {
         if ((off = PyLong_AsLongLong(poff_out)) < 0) goto fail;
         self->dst.fl.off = off;
         self->dst.fl.use_off = 1;
      }
   }
   
   self->l = len;
   return self;
   
   fail:
   Py_DECREF(self);
   return NULL;
}

static void DataTransferRequest_dealloc(DataTransferRequest *self) {
   if (self->dtd) {
      if (self->ttype & SRC_ISMEM)
         PyBuffer_Release(&self->src.mem);
      if (self->ttype & DST_ISMEM)
         PyBuffer_Release(&self->dst.mem);
      
      Py_DECREF(self->py_src);
      Py_DECREF(self->py_dst);
      Py_DECREF(self->opaque);
      Py_DECREF(self->dtd);
   }
   Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* DataTransferRequest_getopaque(DataTransferRequest *self, void *__p) {
   Py_INCREF(self->opaque);
   return self->opaque;
}

static int DataTransferRequest_setopaque(DataTransferRequest *self,
      PyObject *val, void *__p) {
   
   if (!val) {
      PyErr_SetString(PyExc_Exception, "This arg is not deletable.");
      return -1;
   }
   
   Py_DECREF(self->opaque);
   Py_INCREF(val);
   self->opaque = val;
   
   return 0;
}

static PyObject* DataTransferRequest_queue(DataTransferRequest *self) {
   if (self->next != dtr_unqueued) {
      PyErr_SetString(PyExc_Exception, "I've already been queued.");
      return NULL;
   }
   
   pthread_mutex_lock(&self->dtd->reqs_mtx);
   
   self->next = NULL;
   Py_INCREF(self);
   
   *(self->dtd->req_tp) = self;
   self->dtd->req_tp = &self->next;
   self->dtd->reqcount += 1;
   
   pthread_mutex_unlock(&self->dtd->reqs_mtx);
   pthread_cond_signal(&self->dtd->reqs_cond);
   
   Py_RETURN_NONE;
}


static PyMethodDef DataTransferRequest_methods[] = {
   {"queue", (PyCFunction)DataTransferRequest_queue,
    METH_NOARGS, "Queue transfer with associated DTD object."},
   {NULL}  /* Sentinel */
};

static PyGetSetDef DataTransferRequest_getsetters[] = {
   {"opaque", (getter)DataTransferRequest_getopaque,
    (setter)DataTransferRequest_setopaque, "Opaque value", NULL},
   {NULL}  /* Sentinel */
};


static PyTypeObject DataTransferRequestType = {
   PyVarObject_HEAD_INIT(&PyType_Type, 0)
   "_slowfd.DataTransferRequest", /* tp_name */
   sizeof(DataTransferRequest),     /* tp_basicsize */
   0,                         /* tp_itemsize */
   (destructor)DataTransferRequest_dealloc,/* tp_dealloc */
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
   "FD2FD data transfer request.", /* tp_doc */
   0,		              /* tp_traverse */
   0,		              /* tp_clear */
   0,		              /* tp_richcompare */
   0,		              /* tp_weaklistoffset */
   0,		              /* tp_iter */
   0,		              /* tp_iternext */
   DataTransferRequest_methods,  /* tp_methods */
   0,                         /* tp_members */
   DataTransferRequest_getsetters,  /* tp_getset */
   0,                         /* tp_base */
   0,                         /* tp_dict */
   0,                         /* tp_descr_get */
   0,                         /* tp_descr_set */
   0,                         /* tp_dictoffset */
   0,                         /* tp_init */
   0,                         /* tp_alloc */
   (newfunc)DataTransferRequest_new    /* tp_new */
};

static void* thread_work(void *_wt_data) {
   t_wt_data *wt_data = _wt_data;
   DataTransferRequest *req;
   DataTransferDispatcher *dtd = wt_data->dtd;
   char spfd_tok = '\x00';
   
   pthread_mutex_lock(&dtd->reqs_mtx);
   while (wt_data->active) {
      req = dtd->req;
      if (!req) {
         pthread_cond_wait(&dtd->reqs_cond, &dtd->reqs_mtx);
         continue;
      }
      
      if (!(req->next))
         dtd->req_tp = &dtd->req;
      
      dtd->req = req->next;
      dtd->reqcount -= 1;
      pthread_mutex_unlock(&dtd->reqs_mtx);
      
      copy_data(req, wt_data);
      
      pthread_mutex_lock(&dtd->res_mtx);
      req->next = dtd->res;
      dtd->res = req;
      dtd->rescount += 1;
      if (!req->next) write(dtd->spfd[1], &spfd_tok, 1);
      
      pthread_mutex_unlock(&dtd->res_mtx);
      
      pthread_mutex_lock(&dtd->reqs_mtx);
   }
   
   pthread_mutex_unlock(&dtd->reqs_mtx);
   return NULL;
}

static void _dtd_killthreads(DataTransferDispatcher *dtd) {
   size_t i;
   pthread_mutex_lock(&dtd->reqs_mtx);
   for (i = 0; i < dtd->wtcount; i++)
      dtd->wt_data[i].active = 0;
   
   pthread_cond_broadcast(&dtd->reqs_cond);
   pthread_mutex_unlock(&dtd->reqs_mtx);
   
   for (i = 0; i < dtd->wtcount; i++) {
      pthread_join(dtd->wt_data[i].thread, NULL);
      #ifdef __USE_GNU
      close(dtd->wt_data[i].pfd[0]);
      close(dtd->wt_data[i].pfd[1]);
      #endif
   }
}

static DataTransferDispatcher* DataTransferDispatcher_new(PyTypeObject *type,
      PyObject *args, PyObject *kwargs) {
   
   DataTransferDispatcher *self;
   size_t i;
   static char *kwlist[] = {"wt_count", NULL};
   long wtc;
   if (!PyArg_ParseTupleAndKeywords(args, kwargs, "l", kwlist, &wtc)) return NULL;
   if (wtc <= 0) {
      PyErr_SetString(PyExc_ValueError, "Argument 0 must be positive.");
      return NULL;
   }
   
   self = (DataTransferDispatcher*) type->tp_alloc(type, 0);
   if (!self) return self;
   
   self->req = NULL;
   self->req_tp = &self->req;
   self->res = NULL;
   self->reqcount = 0;
   self->rescount = 0;
   self->wtcount = 0;
   
   if (pthread_cond_init(&self->reqs_cond, NULL)) {
      PyErr_SetFromErrno(PyExc_OSError);
      goto fail;
   }
   if (pthread_mutex_init(&self->reqs_mtx, NULL)) {
      PyErr_SetFromErrno(PyExc_OSError);
      goto fail;
   }
   
   self->wt_data = malloc(sizeof(t_wt_data)*wtc);
   if (!self->wt_data) {
      PyErr_NoMemory();
      goto fail;
   }
   
   if (pipe(self->spfd)) {
      PyErr_SetFromErrno(PyExc_OSError);
      goto fail;
   }
   
   if (fcntl(self->spfd[0], F_SETFL, O_NONBLOCK) ||
       fcntl(self->spfd[1], F_SETFL, O_NONBLOCK)) {
      PyErr_SetFromErrno(PyExc_OSError);
      goto fail_p;
   }
   
   for (i = 0; i < wtc; i++) {
      self->wt_data[i].dtd = self;
      self->wt_data[i].active = 1;
      if (!pthread_create(&self->wt_data[i].thread, NULL, thread_work, &self->wt_data[i])) {
         #ifdef __USE_GNU
         if (!pipe(self->wt_data[i].pfd)) continue;
         /* Failed to make pipe */
         PyErr_SetFromErrno(PyExc_OSError);
         
         pthread_mutex_lock(&self->reqs_mtx);
         self->wt_data[i].active = 0;
         pthread_cond_broadcast(&self->reqs_cond);
         pthread_mutex_unlock(&self->reqs_mtx);
         
         pthread_join(self->wt_data[i].thread, NULL);
         #else
         continue;
         #endif
      } else
         /* Failed to spawn thread. */
         PyErr_SetFromErrno(PyExc_OSError);
      
      /* Something went wrong; kill existing worker threads and pipes */
      self->wtcount = i;
      goto fail_p;
   }
   
   self->wtcount = wtc;
   
   return self;
   
   fail_p:
   close(self->spfd[0]);
   close(self->spfd[1]);
   
   fail:
   Py_DECREF(self);
   return NULL;
}

static PyObject* DataTransferDispatcher_get_results(DataTransferDispatcher *self) {
   PyObject *rv;
   Py_ssize_t i;
   DataTransferRequest *req, **req_pn;
   
   pthread_mutex_lock(&self->res_mtx);
   
   rv = PyTuple_New(self->rescount);
   if (!rv) goto out;
   
   for (i = self->rescount-1, req=self->res; i > -1; i--) {
      PyTuple_SET_ITEM(rv, i, (PyObject*) req);
      req_pn = &req->next;
      req = req->next;
      *req_pn = dtr_unqueued;
   }
   
   if (req) {
      PyErr_SetString(PyExc_Exception, "Result structure / rescount mismatch. Bailing out.");
      Py_DECREF(rv);
      rv = NULL;
      goto out;
   }
   
   self->res = NULL;
   self->rescount = 0;
   
   read(self->spfd[0], scratch_buf, sizeof(scratch_buf));
   
   out:
   pthread_mutex_unlock(&self->res_mtx);
   return rv;
}

static PyObject* DataTransferDispatcher_fileno(DataTransferDispatcher *self) {
   return PyLong_FromLong(self->spfd[0]);
}

static PyObject* DataTransferDispatcher_get_request_count(DataTransferDispatcher *self) {
   return PyLong_FromLong(self->reqcount);
}

static void DataTransferDispatcher_dealloc(DataTransferDispatcher *self) {
   DataTransferRequest *req, *req_prev;
   
   _dtd_killthreads(self);
   free(self->wt_data);
   
   for (req = self->req; req;) {
      req_prev = req;
      req = req->next;
      Py_DECREF(req_prev);
   }

   for (req = self->res; req;) {
      req_prev = req;
      req = req->next;
      Py_DECREF(req_prev);
   }

   close(self->spfd[0]);
   close(self->spfd[1]);
   
   pthread_mutex_destroy(&self->reqs_mtx);
   pthread_mutex_destroy(&self->res_mtx);
   pthread_cond_destroy(&self->reqs_cond);
   
   Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyMethodDef DataTransferDispatcher_methods[] = {
   {"get_results", (PyCFunction)DataTransferDispatcher_get_results,
    METH_NOARGS, "Retrieve DTR objects for finished transfers."},
   {"fileno", (PyCFunction)DataTransferDispatcher_fileno,
    METH_NOARGS, "Return FD for read-end of signal pipe."},
   {"get_request_count", (PyCFunction) DataTransferDispatcher_get_request_count,
    METH_NOARGS, "Return number of pending requests."},
   {NULL}  /* Sentinel */
};

static PyTypeObject DataTransferDispatcherType = {
   PyVarObject_HEAD_INIT(&PyType_Type, 0)
   "_slowfd.DataTransferDispatcher", /* tp_name */
   sizeof(DataTransferDispatcher),     /* tp_basicsize */
   0,                         /* tp_itemsize */
   (destructor)DataTransferDispatcher_dealloc, /* tp_dealloc */
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
   "FD2FD data transfer dispatcher.", /* tp_doc */
   0,		              /* tp_traverse */
   0,		              /* tp_clear */
   0,		              /* tp_richcompare */
   0,		              /* tp_weaklistoffset */
   0,		              /* tp_iter */
   0,		              /* tp_iternext */
   DataTransferDispatcher_methods,   /* tp_methods */
   0,                         /* tp_members */
   0,                         /* tp_getset */
   0,                         /* tp_base */
   0,                         /* tp_dict */
   0,                         /* tp_descr_get */
   0,                         /* tp_descr_set */
   0,                         /* tp_dictoffset */
   0,                         /* tp_init */
   0,                         /* tp_alloc */
   (newfunc)DataTransferDispatcher_new /* tp_new */
};

static PyMethodDef module_methods[] = {
    {NULL, NULL, 0, NULL}        /* Sentinel */
};


static struct PyModuleDef _module = {
   PyModuleDef_HEAD_INIT,
   "_slowfd",
   NULL,
   -1,
   module_methods
};

PyMODINIT_FUNC
PyInit__slowfd(void) {
   PyObject *m = PyModule_Create(&_module);
   if (!m) return NULL;
   
   /* SigInfo type setup */
   if (PyType_Ready(&DataTransferRequestType) < 0) return NULL;
   Py_INCREF(&DataTransferRequestType);
   PyModule_AddObject(m, "DataTransferRequest", (PyObject *)&DataTransferRequestType);
   
   /* DataTransferDispatcher type setup */
   if (PyType_Ready(&DataTransferDispatcherType) < 0) return NULL;
   Py_INCREF(&DataTransferDispatcherType);
   PyModule_AddObject(m, "DataTransferDispatcher", (PyObject *)&DataTransferDispatcherType);
      
   return m;
}
