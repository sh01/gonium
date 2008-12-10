#include "Python.h"
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

static PyObject* saved_signals_get(PyObject*, PyObject*);

typedef struct sdarray {
   volatile siginfo_t *data;
   volatile size_t len;
   volatile size_t used;
   volatile sig_atomic_t nonempty;
} sdarray_t;

sdarray_t *volatile sd0, *sd1;
sigset_t ss_hp, ss_all; // signals to store in slot 0

struct signaldata **signal_data, **signal_data_buf;

static sdarray_t* sdarray_new(size_t buflen) {
   sdarray_t *rv = PyMem_Malloc(sizeof(sdarray_t));
   
   if (!(rv->data = PyMem_Malloc(sizeof(siginfo_t)*buflen))) return NULL;
   rv->len = buflen;
   rv->used = 0;
   rv->nonempty = 0;
   return rv;
}

void sig_handler(int sig, siginfo_t *si, void *context) {
   if (sd0->used >= sd0->len) { /* overlow :( */
      /* Store high-priority signals, regardless */
      if (sigismember(&ss_hp, sig) == 1) sd0->data[sd0->used] = *si;
      return;
   }
   sd0->data[sd0->used++] = *si;
   sd0->nonempty = 1;
}

typedef struct {
   PyObject_HEAD
   /* Type-specific fields go here. */
   siginfo_t data;
} SigInfo;


static PyObject * siginfo_getter(SigInfo *self, void *closure) {
  int idx = *((int*)closure);
  
  switch(idx) {
     case 0: return PyLong_FromLong(self->data.si_signo);
     case 1: return PyLong_FromLong(self->data.si_errno);
     case 2: return PyLong_FromLong(self->data.si_code);
     case 3: return PyLong_FromLong(self->data.si_pid);
     case 4: return PyLong_FromLong(self->data.si_uid);
     case 5: return PyLong_FromLong(self->data.si_status);
     case 6: return PyFloat_FromDouble(self->data.si_utime);
     case 7: return PyFloat_FromDouble(self->data.si_stime);
     case 8: return PyLong_FromLong(self->data.si_value.sival_int);
     case 9: return PyLong_FromVoidPtr(self->data.si_value.sival_ptr);
     case 10: return PyLong_FromLong(self->data.si_int);
     case 11: return PyLong_FromVoidPtr(self->data.si_ptr);
     case 12: return PyLong_FromVoidPtr(self->data.si_addr);
     case 13: return PyLong_FromLong(self->data.si_band);
     case 14: return PyLong_FromLong(self->data.si_fd);
  }
  return NULL;
}

/* -- currently defective
int siginfo_asbuf(SigInfo *self, Py_buffer *view, int flags) {
   if (flags & (PyBUF_WRITABLE | PyBUF_STRIDES)) {
      return -1;
   }
   view->buf = &self->data;
   view->len = sizeof(siginfo_t);
   view->readonly = 1;
   view->format = NULL;
   if (flags & PyBUF_ND) {
      view->ndim = 1;
      if (!(view->shape = PyMem_Malloc(sizeof(Py_ssize_t)))) return -1;
      view->shape[0] = sizeof(siginfo_t);
   } else {
      view->ndim = 0;
      view->shape = NULL;
   }
   return 0;
}*/

int siginfo_idx[] = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14};

static PyGetSetDef siginfo_getsetters[] = {
   {"signo", (getter)siginfo_getter, (setter)NULL, "Signal number", siginfo_idx},
   {"errno", (getter)siginfo_getter, (setter)NULL, "An errno value", siginfo_idx+1},
   {"code", (getter)siginfo_getter, (setter)NULL, "Signal code", siginfo_idx+2},
   {"pid", (getter)siginfo_getter, (setter)NULL, "Sending process ID", siginfo_idx+3},
   {"uid", (getter)siginfo_getter, (setter)NULL, "Real user ID of sending process", siginfo_idx+4},
   {"status", (getter)siginfo_getter, (setter)NULL, "Exit value or signal", siginfo_idx+5},
   {"utime", (getter)siginfo_getter, (setter)NULL, "User time consumed", siginfo_idx+6},
   {"stime", (getter)siginfo_getter, (setter)NULL, "System time consumed", siginfo_idx+7},
   {"value_int", (getter)siginfo_getter, (setter)NULL, "Signal value, int", siginfo_idx+8},
   {"value_ptr", (getter)siginfo_getter, (setter)NULL, "Signal value, ptr", siginfo_idx+9},
   {"int", (getter)siginfo_getter, (setter)NULL, "POSIX.1b signal", siginfo_idx+10},
   {"ptr", (getter)siginfo_getter, (setter)NULL, "POSIX.1b signal", siginfo_idx+11},
   {"addr", (getter)siginfo_getter, (setter)NULL, "Memory location which caused fault", siginfo_idx+12},
   {"band", (getter)siginfo_getter, (setter)NULL, "Band event", siginfo_idx+13},
   {"fd", (getter)siginfo_getter, (setter)NULL, "File descriptor", siginfo_idx+14},
   {NULL}  /* Sentinel */
};


static PyTypeObject siginfoType = {
   PyVarObject_HEAD_INIT(&PyType_Type, 0)
   "signal_.SigInfo",         /* tp_name */
   sizeof(SigInfo),           /* tp_basicsize */
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
   "This type is a thin wrapper around C siginfo_t objects.", /* tp_doc */
   0,		              /* tp_traverse */
   0,		              /* tp_clear */
   0,		              /* tp_richcompare */
   0,		              /* tp_weaklistoffset */
   0,		              /* tp_iter */
   0,		              /* tp_iternext */
   0,	                      /* tp_methods */
   0,                         /* tp_members */
   siginfo_getsetters,        /* tp_getset */
   0,                         /* tp_base */
   0,                         /* tp_dict */
   0,                         /* tp_descr_get */
   0,                         /* tp_descr_set */
   0,                         /* tp_dictoffset */
   0,                         /* tp_init */
   0,                         /* tp_alloc */
   PyType_GenericNew          /* tp_new */
};


typedef struct {
   PyObject_HEAD
   /* Type-specific fields go here. */
   sigset_t ss;
} SigSet;


static int SigSet_init(SigSet *self, PyObject *args, PyObject *kwargs) {
   static char *kwlist[] = {NULL};
   void *bogus;
   // faulty!
   if (!PyArg_ParseTupleAndKeywords(args, kwargs, "", kwlist, &bogus)) return -1;
   if (sigemptyset(&self->ss)) {
      PyErr_SetFromErrno(PyExc_SystemError);
      return -1;
   }
   return 0;
}

static PyObject *SigSet_clear(SigSet *self) {
   if (sigemptyset(&self->ss)) return NULL;
   Py_RETURN_NONE;
}

static PyObject *SigSet_fill(SigSet *self) {
   if (sigfillset(&self->ss)) {
      PyErr_SetFromErrno(PyExc_SystemError);
      return NULL;
   }
   Py_RETURN_NONE;
}

static PyObject *SigSet_add(SigSet *self, PyObject *args) {
   int signal;
   if (!PyArg_ParseTuple(args, "i", &signal)) return NULL;
   if (sigaddset(&self->ss, signal)) {
      PyErr_SetFromErrno(PyExc_ValueError);
      return NULL;
   }
   Py_RETURN_NONE;
}

static PyObject *SigSet_remove(SigSet *self, PyObject *args) {
   int signal;
   if (!PyArg_ParseTuple(args, "i", &signal)) return NULL;
   if (sigdelset(&self->ss, signal)) {
      PyErr_SetFromErrno(PyExc_ValueError);
      return NULL;
   }
   Py_RETURN_NONE;
}

static int SigSet_in(SigSet *self, PyObject *arg) {
   int signal, rv;
   if (((signal = (int) PyLong_AsLong(arg)) == -1) && PyErr_Occurred()) return -1;
   rv = sigismember(&self->ss, signal);
   if (rv == 0) return 0;
   if (rv == 1) return 1;
   if (rv == -1) PyErr_SetFromErrno(PyExc_ValueError);
   return -1;
}


static PyMethodDef SigSet_methods[] = {
   {"clear", (PyCFunction)SigSet_clear, METH_NOARGS, "sigemptyset() wrapper: initialize sigset to empty"},
   {"fill", (PyCFunction)SigSet_fill, METH_NOARGS, "sigfillset() wrapper: initialize sigset to full"},
   {"add", (PyCFunction)SigSet_add, METH_VARARGS, "sigaddset() wrapper: add signal to sigset"},
   {"remove", (PyCFunction)SigSet_remove, METH_VARARGS, "sigdelset() wrapper: remove signal from sigset"},
   {NULL}  /* Sentinel */
};

static PySequenceMethods SigSetassequence = {
   NULL,                     /* sq_length */
   NULL,                     /* sq_concat */
   NULL,                     /* sq_repeat */
   NULL,                     /* sq_item */
   NULL,                     /* sq_slice */
   NULL,                     /* sq_ass_item */
   NULL,                     /* sq_ass_slice */
   (objobjproc)SigSet_in,    /* sq_contains */
};

static PyTypeObject SigSetType = {
   PyVarObject_HEAD_INIT(&PyType_Type, 0)
   "signal_.SigSet",          /* tp_name */
   sizeof(SigSet),            /* tp_basicsize */
   0,                         /* tp_itemsize */
   0,                         /* tp_dealloc */
   0,                         /* tp_print */
   0,                         /* tp_getattr */
   0,                         /* tp_setattr */
   0,                         /* tp_compare */
   0,                         /* tp_repr */
   0,                         /* tp_as_number */
   &SigSetassequence,         /* tp_as_sequence */
   0,                         /* tp_as_mapping */
   0,                         /* tp_hash  */
   0,                         /* tp_call */
   0,                         /* tp_str */
   0,                         /* tp_getattro */
   0,                         /* tp_setattro */
   0,                         /* tp_as_buffer */
   Py_TPFLAGS_DEFAULT,        /* tp_flags */
   "This type is a thin wrapper around C sigset_t objects.", /* tp_doc */
   0,		              /* tp_traverse */
   0,		              /* tp_clear */
   0,		              /* tp_richcompare */
   0,		              /* tp_weaklistoffset */
   0,		              /* tp_iter */
   0,		              /* tp_iternext */
   SigSet_methods,            /* tp_methods */
   0,                         /* tp_members */
   0,                         /* tp_getset */
   0,                         /* tp_base */
   0,                         /* tp_dict */
   0,                         /* tp_descr_get */
   0,                         /* tp_descr_set */
   0,                         /* tp_dictoffset */
   (initproc)SigSet_init,     /* tp_init */
   0,                         /* tp_alloc */
   PyType_GenericNew          /* tp_new */
};



static PyMethodDef module_methods[] = {
   {"saved_signals_get", saved_signals_get, METH_VARARGS, "Return tuple containing saved signals."},
   {NULL, NULL, 0, NULL}
};


static struct PyModuleDef signal_module = {
   PyModuleDef_HEAD_INIT,
   "signal_",
   NULL,
   -1,
   module_methods
};


static PyObject* saved_signals_get(PyObject *self, PyObject *args) {
   sdarray_t *sda_tmp;
   siginfo_t *sia;
   sigset_t ss_tmp;
   size_t i, used;
   SigInfo *si_py;
   
   PyObject *rv;
   
   if (!PyArg_ParseTuple(args,"")) return NULL;
   if (!sd0->nonempty) return PyTuple_New(0);
   sigprocmask(SIG_SETMASK, &ss_all, &ss_tmp);
   sda_tmp = sd0;
   sd0 = sd1;
   sd1 = sda_tmp;
   sigprocmask(SIG_SETMASK, &ss_tmp, NULL);
   
   used = sd1->used;
   if (!(rv = PyTuple_New(used))) return NULL;
   sia = (void*) sd1->data;

   for (i = 0; i < used; i++) {
      if (!(si_py = PyObject_New(SigInfo, &siginfoType)) ||
         PyTuple_SetItem(rv, i, (void*) si_py)) {
         /* error handling */
         Py_DECREF(rv);
         return NULL;
      }
      si_py->data = sia[i];
   }
   sd1->used = 0;
   sd1->nonempty = 0;
   return rv;
}


PyMODINIT_FUNC
PyInit_signal_(void) {
   PyObject *m = PyModule_Create(&signal_module);
   if (!m) return NULL;
   
   size_t buflen = 256;
   if (!(sd0 = sdarray_new(buflen)) || !(sd1 = sdarray_new(buflen))) return NULL;
   sigfillset(&ss_all);
   
   /* SigInfo type setup */
   if (PyType_Ready(&siginfoType) < 0) return NULL;
   Py_INCREF(&siginfoType);
   PyModule_AddObject(m, "SigInfo", (PyObject *)&siginfoType);
   
   /* SigSet type setup */
   if (PyType_Ready(&SigSetType) < 0) return NULL;
   Py_INCREF(&SigSetType);
   PyModule_AddObject(m, "SigSet", (PyObject *)&SigSetType);
   
   return m;
}
