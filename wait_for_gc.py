#!/usr/bin/python

"""
This action will wait for the most recent GC cycle to complete

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --timeout           How long to wait (min) before giving up and considering the GC incomplete
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class WaitForGcAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_WAIT = "BEFORE_WAIT"
        GC_TIMEOUT = "GC_TIMEOUT"
        GC_FINISHED = "GC_FINISHED"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None},
            args)

    def Execute(self, mvip=sfdefaults.mvip, timeout=sfdefaults.gc_timeout, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Wait for GC cycle to finish
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster = libsfcluster.SFCluster(mvip, username, password)

        # Wait for a GC cycle
        mylog.info("Waiting for the most recent GC to finish...")
        self._RaiseEvent(self.Events.BEFORE_WAIT)
        try:
            gc_info = cluster.WaitForGC(timeout)
        except libsf.SfTimeoutError:
            mylog.error("Timed out waiting for GC to finish")
            self._RaiseEvent(self.Events.GC_TIMEOUT)
            return False
        except libsf.SfError as e:
            mylog("Failed to wait for GC: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.info("GC generation " + str(gc_info.Generation) + " started " + libsf.TimestampToStr(gc_info.StartTime) + ", duration " + libsf.SecondsToElapsedStr(gc_info.EndTime - gc_info.StartTime) + ", " + libsf.HumanizeBytes(gc_info.DiscardedBytes) + " discarded")
        mylog.info("    " + str(len(gc_info.ParticipatingSSSet)) + " participating SS: " + ",".join(map(str, gc_info.ParticipatingSSSet)) + "  " + str(len(gc_info.EligibleBSSet)) + " eligible BS: " + ",".join(map(str, gc_info.EligibleBSSet)) + "")
        self._RaiseEvent(self.Events.GC_FINISHED)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line options
    parser = OptionParser(description="Add all of the available drives to the cluster and wait for syncing to complete.")
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--timeout", type="int", dest="timeout", default=sfdefaults.gc_timeout, help="how long to wait for GC to complete before giving up")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()
    if extra_args and len(extra_args) > 0:
        mylog.error("Unknown arguments: " + ",".join(extra_args))
        sys.exit(1)

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.timeout, options.username, options.password, options.debug):
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

