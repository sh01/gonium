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

/* Give productive feedback to people trying to link this with Python 2.x */
#if PY_MAJOR_VERSION < 3
#error This file requires python >= 3.0
#endif

static PyObject* saved_signals_get(PyObject*, PyObject*);

typedef struct sdarray {
   volatile siginfo_t *data;
   volatile size_t used;
   volatile sig_atomic_t nonempty;
} sdarray_t;


static size_t sdalen;
static sdarray_t *volatile sd0=NULL, *sd1=NULL;
sigset_t ss_hp, ss_all; // signals to store in slot 0
struct signaldata **signal_data, **signal_data_buf;
static int wakeup_fd = -1;


static sdarray_t* sdarray_new(size_t buflen) {
   sdarray_t *rv = PyMem_Malloc(sizeof(sdarray_t));
   if (!(rv->data = PyMem_Malloc(sizeof(siginfo_t)*buflen))) return NULL;
   rv->used = 0;
   rv->nonempty = 0;
   return rv;
}

static int sdarrays_realloc(size_t buflen) {
   void *a0, *a1;
   sigset_t ss_tmp;
   size_t cpyelcnt;
   if (!(a0 = PyMem_Malloc(sizeof(siginfo_t)*buflen))) return -1;
   if (!(a1 = PyMem_Malloc(sizeof(siginfo_t)*buflen))) {
      PyMem_Free(a0);
      return -1;
   }
   sigprocmask(SIG_SETMASK, &ss_all, &ss_tmp);
   cpyelcnt = sdalen;
   if (cpyelcnt > buflen) cpyelcnt = buflen;
   
   PyMem_Free((void*) sd1->data);
   sd1->data = a1;
   memcpy(a0, (void*) sd0->data, sizeof(siginfo_t)*cpyelcnt);
   PyMem_Free((void*) sd0->data);
   sd0->data = a0;
   sdalen = buflen;
   if (sd0->used > buflen) sd0->used = buflen;
   sigprocmask(SIG_SETMASK, &ss_tmp, NULL);
   return 0;
}

static PyObject* sdas_resize(PyObject *self, PyObject *args) {
   Py_ssize_t len;
   if (!PyArg_ParseTuple(args, "n", &len)) return NULL;
   if (len < 1) {
      PyErr_SetString(PyExc_ValueError, "Argument 0 must be positive.");
      return NULL;
   }
   if (sdarrays_realloc(len)) return NULL;
   Py_RETURN_NONE;
}


static void sig_handler(int sig, siginfo_t *si, void *context) {
   char c = 0;
   
   if (sd0->used >= sdalen) { /* overlow :( */
      /* Store high-priority signals, regardless */
      if (sigismember(&ss_hp, sig) == 1) sd0->data[sd0->used] = *si;
      return;
   }
   sd0->data[sd0->used++] = *si;
   if ((!sd0->nonempty) && (wakeup_fd >= 0)) write(wakeup_fd, &c, 1);
   sd0->nonempty = 1;
}

typedef struct {
   PyObject_HEAD
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


int SigInfo_getbuf(SigInfo *self, Py_buffer *view, int flags) {
   return PyBuffer_FillInfo(view, (PyObject*) self, (void*) &self->data,
                            sizeof(siginfo_t), 0, 0);
}

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


static PyBufferProcs SigInfo_asbuf = {
   (getbufferproc) SigInfo_getbuf,
   NULL
};

static PyTypeObject siginfoType = {
   PyVarObject_HEAD_INIT(&PyType_Type, 0)
   "_signal.SigInfo",         /* tp_name */
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
   &SigInfo_asbuf,            /* tp_as_buffer */
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
   sigset_t ss;
} SigSet;


static int SigSet_init(SigSet *self, PyObject *args, PyObject *kwargs) {
   static char *kwlist[] = {NULL};
   if (!PyArg_ParseTupleAndKeywords(args, kwargs, "", kwlist)) return -1;
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

int SigSet_getbuf(SigSet *self, Py_buffer *view, int flags) {
   return PyBuffer_FillInfo(view, (PyObject*) self, (void*) &self->ss,
                            sizeof(sigset_t), 0, 0);
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

static PyBufferProcs SigSet_asbuf = {
   (getbufferproc) SigSet_getbuf,
   NULL
};

static PyTypeObject SigSetType = {
   PyVarObject_HEAD_INIT(&PyType_Type, 0)
   "_signal.SigSet",          /* tp_name */
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
   &SigSet_asbuf,             /* tp_as_buffer */
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

static PyObject* set_hp_sigset(PyObject *self, PyObject *args) {
   PyObject *arg1 = Py_None;
   SigSet *rv;
   if (!PyArg_ParseTuple(args, "|O", &arg1)) return NULL;
   if (arg1 != Py_None) {
      if (!PyObject_IsInstance(arg1, (void*) &SigSetType)) {
         PyErr_SetString(PyExc_TypeError, "Optional argument 0 must be None or SigSet instance.");
         return NULL;
      }
   }
   if (!(rv = PyObject_New(SigSet, &SigSetType))) return NULL;
   rv->ss = ss_hp;
   if (arg1 != Py_None) ss_hp = ((SigSet*) arg1)->ss;
   return (void*) rv;
}


static PyObject *sighandler_install(SigSet *self, PyObject *args) {
   int sig, flags=0;
   struct sigaction sa;
   
   if (!PyArg_ParseTuple(args, "i|i", &sig, &flags)) return NULL;
   sa.sa_sigaction = sig_handler;
   sa.sa_mask = ss_all;
   sa.sa_flags = (SA_SIGINFO | flags);
   
   if (sigaction(sig, &sa, NULL)) {
      PyErr_SetFromErrno(PyExc_ValueError);
      return NULL;
   }
   Py_RETURN_NONE;
}

/* idea copied from python's signal.set_wakeup_fd */
static PyObject *set_wakeup_fd(SigSet *self, PyObject *args) {
   sigset_t ss_tmp;
   PyObject *arg1;
   int fd_new, fd_old;
   
   if (!PyArg_ParseTuple(args, "O", &arg1)) return NULL;
   if (arg1 == Py_None) fd_new = -1;
   else if (((fd_new = PyObject_AsFileDescriptor(arg1)) == -1) && PyErr_Occurred())
      return NULL;
   
   sigprocmask(SIG_SETMASK, &ss_all, &ss_tmp);
   fd_old = wakeup_fd;
   wakeup_fd = fd_new;
   sigprocmask(SIG_SETMASK, &ss_tmp, NULL);
   
   return PyLong_FromLong(fd_old);
}


static PyMethodDef module_methods[] = {
   {"saved_signals_get", saved_signals_get, METH_VARARGS,
    "saved_signals_get() -> signals \n\
     Return tuple containing saved signals."},
   {"set_wakeup_fd", (PyCFunction)set_wakeup_fd, METH_VARARGS,
    "set_wakeup_fd(fd:int) -> int\n\
     Set new fd to write to when a signal is caught after last call to\n\
     saved_signals_get(); returns old value. Specify a negative value\n\
     or None to disable."},
   {"set_hp_sigset", set_hp_sigset, METH_VARARGS,
    "set_hp_sigset(signals:SigSet) -> SigSet\n\
     Set new 'highpriority' signalset. Signals in this set will be saved,\n\
     even after the buffer has been filled; for this purpose, the last saved\n\
     signal will be overwritten."},
   {"sighandler_install", (PyCFunction)sighandler_install, METH_VARARGS,
    "sighandler_install(signal:int, flags:int=0) -> NoneType\n\
     Start catching specified signal. Flags are as for this system's\n\
     sigaction()."},
   {"sd_buffers_resize" , sdas_resize, METH_VARARGS,
    "sd_buffers_resize(count:int) -> NoneType\n\
     Resize signal data buffers to a size sufficent to store <count> signals."},
   {NULL, NULL, 0, NULL}
};


static struct PyModuleDef _signalmodule = {
   PyModuleDef_HEAD_INIT,
   "_signal",
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
   
   PyObject *psd, *rv;
   if (!PyArg_ParseTuple(args,"")) return NULL;
   if (!sd0->nonempty) return Py_BuildValue("Ni", PyTuple_New(0), 0);
   sda_tmp = sd1;
   sd1 = sd0;
   sigprocmask(SIG_SETMASK, &ss_all, &ss_tmp);
   sd0 = sda_tmp;
   sigprocmask(SIG_SETMASK, &ss_tmp, NULL);
   
   used = sd1->used;
   if (!(psd = PyTuple_New(used))) return NULL;
   sia = (void*) sd1->data;

   for (i = 0; i < used; i++) {
      if (!(si_py = PyObject_New(SigInfo, &siginfoType)) ||
         PyTuple_SetItem(psd, i, (void*) si_py)) {
         /* error handling */
         Py_DECREF(psd);
         return NULL;
      }
      si_py->data = sia[i];
   }
   rv = Py_BuildValue("Ni", psd, (sd1->used >= sdalen) ? 1 : 0);
   
   sd1->used = 0;
   sd1->nonempty = 0;
   
   return rv;
}


PyMODINIT_FUNC
PyInit__signal(void) {
   PyObject *m = PyModule_Create(&_signalmodule);
   if (!m) return NULL;
   
   if (!sd0 && !sd1) {
      sdalen = 256;
      if (!(sd0 = sdarray_new(sdalen))) return NULL;
      if (!(sd1 = sdarray_new(sdalen))) return NULL;
      sigfillset(&ss_all);
      sigemptyset(&ss_hp);
   }
   
   /* SigInfo type setup */
   if (PyType_Ready(&siginfoType) < 0) return NULL;
   Py_INCREF(&siginfoType);
   PyModule_AddObject(m, "SigInfo", (PyObject *)&siginfoType);
   
   /* SigSet type setup */
   if (PyType_Ready(&SigSetType) < 0) return NULL;
   Py_INCREF(&SigSetType);
   PyModule_AddObject(m, "SigSet", (PyObject *)&SigSetType);
   
   /* sigaction() posix constants */
   PyModule_AddObject(m, "SA_NOCLDSTOP", PyLong_FromLong(SA_NOCLDSTOP));
   PyModule_AddObject(m, "SA_ONSTACK", PyLong_FromLong(SA_ONSTACK));
   PyModule_AddObject(m, "SA_RESETHAND", PyLong_FromLong(SA_RESETHAND));
   PyModule_AddObject(m, "SA_RESTART", PyLong_FromLong(SA_RESTART));
   PyModule_AddObject(m, "SA_SIGINFO", PyLong_FromLong(SA_SIGINFO));
   PyModule_AddObject(m, "SA_NOCLDWAIT", PyLong_FromLong(SA_NOCLDWAIT));
   PyModule_AddObject(m, "SA_NODEFER", PyLong_FromLong(SA_NODEFER));
   /* sigprocmask() posix constants. These aren't currently directly useful
      for using this module, but are very much so for
      sigprocmask-through-ctypes usage. Since this relates to the primary
      functions of this module, and exposing them is effectively free, we do
      so.
    */
   PyModule_AddObject(m, "SIG_BLOCK", PyLong_FromLong(SIG_BLOCK));
   PyModule_AddObject(m, "SIG_SETMASK", PyLong_FromLong(SIG_SETMASK));
   PyModule_AddObject(m, "SIG_UNBLOCK", PyLong_FromLong(SIG_UNBLOCK));
   
   return m;
}
