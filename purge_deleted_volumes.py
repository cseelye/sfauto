#!/usr/bin/python

"""
This action will purge the deleted volumes on the cluster

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
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class PurgeDeletedVolumesAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_PURGE = "BEFORE_PURGE"
        AFTER_PURGE = "AFTER_PURGE"
        PURGE_VOLUME = "PURGE_VOLUME"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None},
            args)

    def Execute(self, mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Purge the deleted volumes on the cluster
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Get a list of volumes to purge
        try:
            deleted_volumes = libsf.CallApiMethod(mvip, username, password, "ListDeletedVolumes", {})
        except libsf.SfError as e:
            mylog.error("Failed to get volume list: " + e.message)
            return False

        mylog.info("Purging " + str(len(deleted_volumes["volumes"])) + " volumes")
        self._RaiseEvent(self.Events.BEFORE_PURGE)

        # Purge the volumes
        allgood = True
        for vol in deleted_volumes["volumes"]:
            params = {}
            params["volumeID"] = vol["volumeID"]
            self._RaiseEvent(self.Events.PURGE_VOLUME, volumeID=vol["volumeID"])
            try:
                libsf.CallApiMethod(mvip, username, password, "PurgeDeletedVolume", params, ExitOnError=False)
            except libsf.SfError as e:
                mylog.error("Could not purge volume " + str(vol["volumeID"]) + ": " + str(e))
                self.RaiseFailureEvent(message="Could not purge volume " + str(vol["volumeID"]) + ": " + str(e), volumeID=vol["volumeID"], exception=e)
                allgood = False

        self._RaiseEvent(self.Events.AFTER_PURGE)
        if allgood:
            return True
        else:
            return False

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

