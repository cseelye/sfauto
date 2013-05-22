#!/usr/bin/python

"""
This action will wait for there to be no bin syncing on the cluster

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --timeout           How long to wait (sec) before giving up
"""

import sys
from optparse import OptionParser
import time
import logging
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class WaitForBinsyncAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_WAIT = "BEFORE_WAIT"
        WAIT_TIMEOUT = "WAIT_TIMEOUT"
        BIN_SYNC_FINISHED = "BIN_SYNC_FINISHED"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "timeout" : libsf.IsPositiveInteger},
            args)

    def Execute(self, timeout=sfdefaults.bin_sync_timeout, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Wait for no bin syncing on the cluster
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Waiting for there to be no bin syncing on " + mvip)
        cluster = libsfcluster.SFCluster(mvip, username, password)
        start_time = time.time()
        self._RaiseEvent(self.Events.BEFORE_WAIT)
        while True:
            try:
                syncing = cluster.IsBinSyncing()
            except libsf.SfError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            if not syncing:
                break

            time.sleep(30)
            if time.time() - start_time > timeout:
                mylog.error("Timeout waiting for bin syncing")
                self._RaiseEvent(self.Events.WAIT_TIMEOUT)
                return False

        end_time = time.time()
        duration = end_time - start_time

        mylog.info("Duration " + libsf.SecondsToElapsedStr(duration))
        mylog.passed("Bin syncing is finished")
        self._RaiseEvent(self.Events.BIN_SYNC_FINISHED)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--timeout", type="int", dest="timeout", default=sfdefaults.bin_sync_timeout, help="how long to wait (sec) before giving up [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.timeout, options.mvip, options.username, options.password, options.debug):
            sys.exit(0)
        else:
            sys.exit(1)
    except libsf.SfArgumentError as e:
        mylog.error("Invalid arguments - \n" + str(e))
        sys.exit(1)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        Abort()
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

