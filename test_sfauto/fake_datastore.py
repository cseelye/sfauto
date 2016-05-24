#!/usr/bin/env python2.7

import multiprocessing
import os
import cPickle as pickle
import threading

class MultiprocessDatastore(object):

    def __init__(self, filename="test_datastore.pickle", reuse=False):
        self.lock = multiprocessing.Lock()
        self.filename = os.path.join("test", filename)
        if not os.path.exists(self.filename) or not reuse:
            with open(self.filename, "w+") as backingfile:
                pickle.dump({}, backingfile)

    def __enter__(self):
        self.lock.acquire()
        return self

    def __exit__(self, extype, exvalue, traceback):
        self.lock.release()

    def Get(self, keyname):
        with open(self.filename, "r") as backingfile:
            ds = pickle.load(backingfile)
        return ds.get(keyname)
    
    def Set(self, keyname, value):
        with open(self.filename, "r") as backingfile:
            ds = pickle.load(backingfile)
        ds[keyname] = value
        with open(self.filename, "w+") as backingfile:
            pickle.dump(ds, backingfile)


class InMemoryDatastore(object):

    def __init__(self):
        self.lock = threading.Lock()
        self.data = {}

    def Get(self, keyname):
        return self.data.get(keyname)

    def Set(self, keyname, value):
        self.data[keyname] = value

    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, extype, exvalue, traceback):
        self.lock.release()


