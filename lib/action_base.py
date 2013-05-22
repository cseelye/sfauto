import sys
import libsf
from libsf import mylog
import datastore
import sfdefaults

class ActionBase(object):
    """
    Base class for all actions
    """

    def __init__(self, events):
        # Container for any threads/processes this action uses
        self._threads = []

        # Setup hook table
        self.HookNames = []
        self._events = {}
        for n in dir(events):
            if n.startswith("__"):
                continue
            self.HookNames.append(n)
            self._events[getattr(events, n)] = None

    def ListAvailableHooks(self):
        """
        Get a list of the hooks this action provides
        """
        return sorted(self._events.keys())

    def RegisterEventCallback(self, event, callback):
        """
        Register a hook callback
        """
        if event not in self._events:
            raise libsf.SfError("Hook '" + event + "' is not defined for this module")
        self._events[event] = libsf.CallbackWrapper(callback)

    def _RaiseEvent(self, event, *args, **kwargs):
        if self._events.get(event):
            mylog.debug("Executing event handler for " + str(event))
            self._events[event](*args, **kwargs)

    def RaiseFailureEvent(self, *args, **kwargs):
        if not getattr(self.Events, "FAILURE"):
            return
        self._RaiseEvent(self.Events.FAILURE, *args, **kwargs)
        if sfdefaults.stop_on_error:
            self.Abort()
            sys.exit(1)

    def Abort(self):
        """
        Abort the execution of this action
        """
        if self._threads:
            mylog.warning("Terminating all threads")
            for th in self._threads:
                th.terminate()
                th.join()

    def SetSharedValue(self, keyName, value):
        """
        Set a value in the shared datastore
        """
        datastore.Set(keyName, value)

    def GetSharedValue(self, keyName):
        """
        Get a value from the shared datastore
        """
        return datastore.Get(keyName)

    def GetNextSharedValue(self, keyName):
        """
        Get the next item from a shared list value
        """
        try:
            datastore.Lock()
            items = datastore.Get(keyName)
            if not items:
                return None
            indexName = keyName + "Name"
            indexValue = datastore.Get(indexName)
            if indexValue is None:
                indexValue = 0
            else:
                indexValue += 1
            datastore.Set(indexName, indexValue)
            return items[indexValue % len(items)]
        finally:
            datastore.Unlock()

    def DelSharedValue(self, keyName):
        """
        Delete a value from the shared datastore
        """
        datastore.Del(keyName)

