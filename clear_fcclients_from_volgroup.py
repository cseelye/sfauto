#!/usr/bin/python

"""
This action will remove all FC clients from a volume group

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --volgroup_name     The name of the volume group

    --volgroup_id       The ID of the volume group
"""

import sys
from optparse import OptionParser
import logging
import lib.libsf as libsf
from lib.libsf import mylog, SfError
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.libsfcluster import SFCluster

class ClearFcclientsFromVolgroupAction(ActionBase):
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
                            "password" : None,
                            },
            args)
        if not args["volgroup_name"] and args["volgroup_id"] <= 0:
            raise libsf.SfArgumentError("Please specify a volgroup name or ID")

    def Execute(self, mvip=sfdefaults.mvip, volgroup_name=None, volgroup_id=0, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Remove all FC clients from the specified volume access group
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        # Get the cluster version so we know which endpoint to use
        cluster = SFCluster(mvip, username, password)
        try:
            api_version = cluster.GetAPIVersion()
        except libsf.SfError as e:
            mylog.error("Failed to get cluster version: " + str(e))
            mylog.info("Assuming API version 7.0")
            api_version = 7.0

        # Find the group
        mylog.info("Finding the volume group on the cluster")
        try:
            volgroup = libsf.FindVolumeAccessGroup(mvip, username, password, VagName=volgroup_name, VagId=volgroup_id, ApiVersion=api_version)
        except libsf.SfError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Separate initiators into FC and iSCSI
        if "iscsiInitiators" not in volgroup:
            volgroup["iscsiInitiators"] = []
        if "fibreChannelInitiators" not in volgroup:
            volgroup["fibreChannelInitiators"] = []
        if "initiators" in volgroup:
            for init in volgroup["initiators"]:
                if init.startswith("iqn") and init not in volgroup["iscsiInitiators"]:
                    volgroup["iscsiInitiators"].append(init)
                elif init not in volgroup["fibreChannelInitiators"]:
                    volgroup["fibreChannelInitiators"].append(init)

        mylog.info("Removing all FC WWNs from group")
        params = {}
        params["volumeAccessGroupID"] = volgroup["volumeAccessGroupID"]
        params["fibreChannelInitiators"] = []
        params["initiators"] = volgroup["iscsiInitiators"]
        try:
            libsf.CallApiMethod(mvip, username, password, "ModifyVolumeAccessGroup", params, ApiVersion=api_version)
        except libsf.SfApiError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.passed("Successfully removed FC clients from group")
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster [%default]")
    parser.add_option("--volgroup_name", type="string", dest="volgroup_name", default=None, help="the name of the group")
    parser.add_option("--volgroup_id", type="int", dest="volgroup_id", default=0, help="the ID of the group")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.volgroup_name, options.volgroup_id, options.username, options.password, options.debug):
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
