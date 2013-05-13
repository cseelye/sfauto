import libsf
from libsf import mylog

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

    def Abort(self):
        """
        Abort the execution of this action
        """
        if self._threads:
            mylog.warning("Terminating all threads")
            for th in self._threads:
                th.terminate()
                th.join()

