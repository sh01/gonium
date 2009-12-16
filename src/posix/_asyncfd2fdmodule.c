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

static char scratch_buf[10240];

static PyTypeObject DataTransferDispatcherType;
struct __DataTransferDispatcher;
struct _t_dtj;

typedef struct __t_wt_data {
   struct __DataTransferDispatcher *dtd;
   pthread_t thread;
   int active;
   #ifdef __USE_GNU
   int pfd[2];
   #endif
} t_wt_data;

#ifdef __USE_GNU
typedef loff_t lt_off;
void static copy_data(int ifd, int ofd, lt_off *off_in, lt_off *off_out,
      size_t l, t_wt_data *wt_data) {
   long e;
   unsigned int dflags = SPLICE_F_MOVE;
   
   while (l) {
      e = splice(ifd, off_in, wt_data->pfd[1], NULL, l,
         SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
      if (e < 0) abort();
      l -= e;
      if (l) dflags |= SPLICE_F_MORE;
      e = splice(wt_data->pfd[0], NULL, ofd, off_out, e, dflags);
      if (e < 0) abort();
   }
}

#else
typedef off_t lt_off;
ssize_t static copy_data(int ifd, int ofd, lt_off *p_off_in, lt_off *p_off_out, size_t l) {
   ssize_t rv;
   lt_off off_in = p_off_in ? *p_off_in : 0;
   lt_off off_out = p_off_out ? *p_off_out : 0;
   
   char *buf;
   buf = malloc(l);
   if (!buf) abort();
   rv = pread(ifd, buf, l, off_in);
   if (rv < 0) {
      free(buf);
      return rv;
   }
   rv = pwrite(ofd, buf l, off_out);
   free(buf);
   return rv;
}
#endif

typedef struct {
   PyObject_HEAD
   struct __DataTransferDispatcher *dtd;
   PyObject *src_file, *dst_file, *opaque;
   
   int src_fd, dst_fd;
   lt_off src_off, dst_off;
   lt_off *p_src_off, *p_dst_off;
   size_t l;
} DataTransferRequest;

typedef struct _t_dtj {
   DataTransferRequest *dtr;
   struct _t_dtj *next;
} t_dtj;

typedef struct __DataTransferDispatcher {
   PyObject_HEAD
   pthread_mutex_t reqs_mtx, res_mtx;
   pthread_cond_t reqs_cond;
   t_wt_data *wt_data; /* worker thread data */
   long wtcount;       /* worker thread count */
   t_dtj *req, *res;   /* requests, results */
   size_t reqcount;    /* request count */
   size_t rescount;    /* result count */
   t_dtj **req_tp;     /* request tail pointer */
   int spfd[2];        /* signal pipe */
} DataTransferDispatcher;


static DataTransferRequest* DataTransferRequest_new(PyTypeObject *type,
      PyObject *args, PyObject *kwargs) {
   
   DataTransferRequest *self;
   static char *kwlist[] = {"dtd", "file_in", "file_out", "off_in", "off_out",
      "length", "opaque", NULL};
   PyObject *fl_in, *fl_out, *poff_in, *poff_out, *opaque;
   DataTransferDispatcher *dtd;
   long long off, len;
   
   
   if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!OOOOLO", kwlist,
      &DataTransferDispatcherType, &dtd,
      &fl_in, &fl_out, &poff_in, &poff_out, &len, &opaque)) return NULL;
   
   self = (DataTransferRequest*) type->tp_alloc(type, 0);
   if (!self) return self;
   self->dtd = NULL;
   
   if ((self->src_fd = PyObject_AsFileDescriptor(fl_in)) < 0) goto fail;
   if ((self->dst_fd = PyObject_AsFileDescriptor(fl_out)) < 0) goto fail;
   
   /* XXX: Potential for silent overflows here if lt_off is 32bit. Mitigate that somehow? */
   if (poff_in == Py_None) {
      self->p_src_off = NULL;
   } else {
      if ((off = PyLong_AsLongLong(poff_in)) < 0) goto fail;
      self->src_off = off;
      self->p_src_off = &self->src_off;
   }
   
   if (poff_out == Py_None) {
      self->p_dst_off = NULL;
   } else {
      if ((off = PyLong_AsLongLong(poff_out)) < 0) goto fail;
      self->dst_off = off;
      self->p_dst_off = &self->dst_off;
   }
   
   self->l = len;
   
   Py_INCREF(fl_in);
   self->src_file = fl_in;
   Py_INCREF(fl_out);
   self->dst_file = fl_out;
   Py_INCREF(dtd);
   self->dtd = dtd;
   Py_INCREF(opaque);
   self->opaque = opaque;
   
   return self;
   
   fail:
   Py_DECREF(self);
   return NULL;
}

static void DataTransferRequest_dealloc(DataTransferRequest *self) {
   if (self->dtd) {
      Py_DECREF(self->src_file);
      Py_DECREF(self->dst_file);
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
   t_dtj *job;
   
   pthread_mutex_lock(&self->dtd->reqs_mtx);
   
   job = malloc(sizeof(t_dtj));
   if (!job) {
      pthread_mutex_unlock(&self->dtd->reqs_mtx);
      return PyErr_NoMemory();
   }
   
   job->dtr = self;
   job->next = NULL;
   Py_INCREF(self);
   
   *(self->dtd->req_tp) = job;
   self->dtd->req_tp = &job->next;
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
   "_asyncfd2fd.DataTransferRequest", /* tp_name */
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
   t_dtj *job;
   DataTransferDispatcher *dtd = wt_data->dtd;
   DataTransferRequest *dtr;
   char spfd_tok = '\x00';
   
   pthread_mutex_lock(&dtd->reqs_mtx);
   while (wt_data->active) {
      job = dtd->req;
      if (!job) {
         pthread_cond_wait(&dtd->reqs_cond, &dtd->reqs_mtx);
         continue;
      }
      
      if (!(job->next))
         dtd->req_tp = &dtd->req;
      
      dtd->req = job->next;
      dtd->reqcount -= 1;
      pthread_mutex_unlock(&dtd->reqs_mtx);
      
      dtr = job->dtr;
      copy_data(dtr->src_fd, dtr->dst_fd, dtr->p_src_off, dtr->p_dst_off,
         dtr->l, wt_data);
      
      pthread_mutex_lock(&dtd->res_mtx);
      job->next = dtd->res;
      dtd->res = job;
      dtd->rescount += 1;
      if (!job->next) write(dtd->spfd[1], &spfd_tok, 1);
      
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
      PyErr_SetFromErrno(PyExc_SystemError);
      goto fail;
   }
   if (pthread_mutex_init(&self->reqs_mtx, NULL)) {
      PyErr_SetFromErrno(PyExc_SystemError);
      goto fail;
   }
   
   self->wt_data = malloc(sizeof(t_wt_data)*wtc);
   if (!self->wt_data) {
      PyErr_NoMemory();
      goto fail;
   }
   
   if (pipe(self->spfd)) {
      PyErr_SetFromErrno(PyExc_SystemError);
      goto fail;
   }
   
   if (fcntl(self->spfd[0], F_SETFL, O_NONBLOCK) ||
       fcntl(self->spfd[1], F_SETFL, O_NONBLOCK)) {
      PyErr_SetFromErrno(PyExc_SystemError);
      goto fail_p;
   }
   
   for (i = 0; i < wtc; i++) {
      self->wt_data[i].dtd = self;
      self->wt_data[i].active = 1;
      if (!pthread_create(&self->wt_data[i].thread, NULL, thread_work, &self->wt_data[i])) {
         #ifdef __USE_GNU
         if (!pipe(self->wt_data[i].pfd)) continue;
         /* Failed to make pipe */
         PyErr_SetFromErrno(PyExc_SystemError);
         
         pthread_mutex_lock(&self->reqs_mtx);
         self->wt_data[i].active = 0;
         pthread_cond_broadcast(&self->reqs_cond);
         pthread_mutex_unlock(&self->reqs_mtx);
         
         pthread_join(self->wt_data[i].thread, NULL);
         #else
         continue
         #endif
      } else
         /* Failed to spawn thread. */
         PyErr_SetFromErrno(PyExc_SystemError);
      
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
   t_dtj *job, *job_last;
   
   pthread_mutex_lock(&self->res_mtx);
   
   rv = PyTuple_New(self->rescount);
   if (!rv) goto out;
   
   for (i = self->rescount-1, job=self->res; i > -1; i--) {
      PyTuple_SET_ITEM(rv, i, (PyObject*) job->dtr);
      job_last = job;
      job = job->next;
      free(job_last);
   }
   
   if (job) {
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
   t_dtj *job, *job_last;
   
   _dtd_killthreads(self);
   free(self->wt_data);
   
   for (job = self->req; job;) {
      job_last = job;
      job = job->next;
      Py_DECREF(job->dtr);
      free(job_last);
   }
   for (job = self->res; job;) {
      job_last = job;
      job = job->next;
      Py_DECREF(job->dtr);
      free(job_last);
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
   "_asyncfd2fd.DataTransferDispatcher", /* tp_name */
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


static struct PyModuleDef _asyncfd2fdmodule = {
   PyModuleDef_HEAD_INIT,
   "_asyncfd2fd",
   NULL,
   -1,
   module_methods
};

PyMODINIT_FUNC
PyInit__asyncfd2fd(void) {
   PyObject *m = PyModule_Create(&_asyncfd2fdmodule);
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
