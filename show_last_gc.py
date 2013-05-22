#!/usr/bin/python

"""
This action will display information abount the last GC

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

class ShowLastGcAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
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
        Get the last GC info
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster = libsfcluster.SFCluster(mvip, username, password)

        gc_info = cluster.GetLastGCInfo()
        incomplete = gc_info.EligibleBSSet - gc_info.CompletedBSSet

        if gc_info.EndTime > 0:
            mylog.info("Last GC started at " + libsf.TimestampToStr(gc_info.StartTime) + ", duration " + libsf.SecondsToElapsedStr(gc_info.EndTime - gc_info.StartTime) + ", " + libsf.HumanizeBytes(gc_info.DiscardedBytes) + " discarded")
            mylog.info(str(len(gc_info.ParticipatingSSSet)) + " participating SS: " + ",".join(map(str, gc_info.ParticipatingSSSet)) + "  " + str(len(gc_info.EligibleBSSet)) + " eligible BS: " + ",".join(map(str, gc_info.EligibleBSSet)) + "")

            if len(incomplete) > 0:
                mylog.warning("Services " + ", ".join(map(str, incomplete)) + " did not complete GC")
                self._RaiseEvent(self.Events.GC_INCOMPLETE, GCInfo=gc_info)
            else:
                self._RaiseEvent(self.Events.GC_FINISHED, GCInfo=gc_info)
        else:
            mylog.error("Last GC started at " + libsf.TimestampToStr(gc_info.StartTime) + " but did not complete")
            self._RaiseEvent(self.Events.GC_INCOMPLETE, GCInfo=gc_info)
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

