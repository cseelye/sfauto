#!/usr/bin/python

"""
This action will display info about all of the GC cycles

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var
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

class ShowGcInfoAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        GC_RESCHEDULED = "GC_RESCHEDULED"
        GC_INCOMPLETE = "GC_INCOMPLETE"
        GC_FINISHED = "GC_FINISHED"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None},
            args)

    def Execute(self, mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Show info about each GC cycle
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster = libsfcluster.SFCluster(mvip, username, password)
        try:
            gc_list = cluster.GetAllGCInfo()
        except libsf.SfError as e:
            mylog.error(str(e))
            return False

        for gc_info in gc_list:
            if gc_info.Rescheduled:
                mylog.warning("GC generation " + str(gc_info.Generation) + " started " + libsf.TimestampToStr(gc_info.StartTime) + " was rescheduled")
                self._RaiseEvent(self.Events.GC_RESCHEDULED, GCInfo=gc_info)
            elif gc_info.EndTime <= 0:
                mylog.warning("GC generation " + str(gc_info.Generation) + " started " + libsf.TimestampToStr(gc_info.StartTime) + " did not complete ")
                mylog.warning("    " + str(len(gc_info.ParticipatingSSSet)) + " participating SS: [" + ",".join(map(str, gc_info.ParticipatingSSSet)) + "]  " + str(len(gc_info.EligibleBSSet)) + " eligible BS: [" + ",".join(map(str, gc_info.EligibleBSSet)) + "]")
                mylog.warning("    " + str(len(gc_info.EligibleBSSet - gc_info.CompletedBSSet)) + " BS did not complete GC: [" + ", ".join(map(str, gc_info.EligibleBSSet - gc_info.CompletedBSSet)) + "]")
                self._RaiseEvent(self.Events.GC_INCOMPLETE, GCInfo=gc_info)
            else:
                mylog.info("GC generation " + str(gc_info.Generation) + " started " + libsf.TimestampToStr(gc_info.StartTime) + ", duration " + libsf.SecondsToElapsedStr(gc_info.EndTime - gc_info.StartTime) + ", " + libsf.HumanizeBytes(gc_info.DiscardedBytes) + " discarded")
                mylog.info("    " + str(len(gc_info.ParticipatingSSSet)) + " participating SS: " + ",".join(map(str, gc_info.ParticipatingSSSet)) + "  " + str(len(gc_info.EligibleBSSet)) + " eligible BS: " + ",".join(map(str, gc_info.EligibleBSSet)) + "")
                self._RaiseEvent(self.Events.GC_FINISHED, GCInfo=gc_info)
            mylog.info("")
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
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.debug):
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
