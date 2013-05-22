#!/usr/bin/python

"""
This action will wait for the cluster to be at least as full as specified

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --full              How full (GB) to wait for
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

class WaitForClusterFullnessAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_WAIT = "BEFORE_WAIT"
        WAIT_TIMEOUT = "WAIT_TIMEOUT"
        CLUSTER_FILLED = "CLUSTER_FILLED"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "full" : libsf.IsInteger,
                            "timeout" : libsf.IsPositiveInteger},
            args)

    def Execute(self, full, timeout=sfdefaults.fill_timeout, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Wait for cluster to fill to specified level
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Waiting for cluster at " + mvip + " to be at least " + str(full) + " GB used")
        cluster = libsfcluster.SFCluster(mvip, username, password)
        start_time = time.time()
        self._RaiseEvent(self.Events.BEFORE_WAIT)
        previous_fullness = -1
        while True:
            current_fullness = cluster.GetClusterUsedSpace()

            if previous_fullness < 0 or ( current_fullness != previous_fullness and current_fullness - previous_fullness > 10 * 1000 * 1000 * 1000):
                mylog.info("  Cluster used space is " + libsf.HumanizeDecimal(current_fullness, 1, "G") + "B")

            if current_fullness >= full * 1000 * 1000 * 1000:
                break

            time.sleep(30)
            if time.time() - start_time > timeout:
                mylog.error("Timeout waiting for bin syncing")
                self._RaiseEvent(self.Events.WAIT_TIMEOUT)
                return False

        end_time = time.time()
        duration = end_time - start_time

        mylog.info("Finished waiting")
        mylog.info("Cluster used space is " + libsf.HumanizeDecimal(current_fullness, 1, "G") + "B")
        mylog.info("Duration " + libsf.SecondsToElapsedStr(duration))
        mylog.passed("Cluster is filled to specified level")
        self._RaiseEvent(self.Events.CLUSTER_FILLED)
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
    parser.add_option("--full", type="int", dest="full", default=0, help="how full (GB) to wait for")
    parser.add_option("--timeout", type="int", dest="timeout", default=sfdefaults.fill_timeout, help="how long to wait (sec) before giving up [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.full, options.timeout, options.mvip, options.username, options.password, options.debug):
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

