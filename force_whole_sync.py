#!/usr/bin/python

"""
This action will force a whole file sync on a list of volumes

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --volume_names      A list of volumes to sync

    --volume_ids        A list of volumes to sync
"""

import sys
from optparse import OptionParser
import json
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class ForceWholeSyncAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None},
            args)
        if not args["volume_names"] and not args["volume_ids"]:
            raise libsf.SfArgumentError("Please supply either volume_names or volume_ids")

    def Execute(self, mvip=sfdefaults.mvip, volume_names=None, volume_ids=None, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Force a while file sync for a list of volumes
        """

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Searching for volumes")
        try:
            found_volumes = libsf.SearchForVolumes(mvip, username, password, VolumeName=volume_names, VolumeId=volume_ids)
        except libsf.SfError as e:
            mylog.error(e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        volume_ids = found_volumes.values()

        if len(volume_ids) <= 0:
            mylog.error("Could not find any matching volumes")
            return False

        # Find the primary and secondary SS for each volume, and force a sync from primary to secondary
        mylog.info("Finding primary/secondary SS")
        try:
            slice_report = libsf.HttpRequest("https://" + str(mvip) + "/reports/slices.json", username, password)
        except libsf.SfError as e:
            mylog.error("Failed to get slice assignments: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        slice_json = json.loads(slice_report)
        for volume_id in volume_ids:
            for slice_obj in slice_json["slices"]:
                if slice_obj["volumeID"] == volume_id:
                    primary = slice_obj["primary"]
                    secondary = slice_obj["liveSecondaries"][0]
                    mylog.info("Forcing whole sync of volume " + str(volume_id) + " from slice" + str(primary) + " to slice" + str(secondary))
                    params = {}
                    params["sliceID"] = volume_id
                    params["primary"] = primary
                    params["secondary"] = secondary
                    try:
                        libsf.CallApiMethod(mvip, username, password, "ForceWholeFileSync", params, ApiVersion=5.0)
                    except libsf.SfError as e:
                        mylog.error("Failed to sync volume: " + str(e))
                        return False
                    break
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--volume_names", type="string", dest="volume_names", default=None, help="the volume to sync")
    parser.add_option("--volume_ids", type="string", dest="volume_ids", default=None, help="the volume to sync")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.volume_names, options.volume_ids, options.username, options.password, options.debug):
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
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)

