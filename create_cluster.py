#!/usr/bin/python

"""
This action will create a cluster

When run as a script, the following options/env variables apply:
    --node_ip           The IP address of the node to use to create the cluster

    --mvip              The management VIP of the cluster
    SFMVIP env var

    --svip              The storage VIP of the cluster
    SFSVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --node_count        How many nodes to wait for before creating the cluster

    --add_drives        Add available drives to the cluster after creation

    --drive_count       How many available drives to wait for before adding
"""

import sys
from optparse import OptionParser
import time
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CreateClusterAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"node_ip" : libsf.IsValidIpv4Address,
                            "node_count" : libsf.IsPositiveInteger,
                            "drive_count" : libsf.IsPositiveInteger,
                            "mvip" : libsf.IsValidIpv4Address,
                            "svip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None},
            args)

    def Execute(self, node_ip, mvip=sfdefaults.mvip, svip=sfdefaults.svip, username=sfdefaults.username, password=sfdefaults.password, add_drives=True, drive_count=0, node_count=0, debug=False):
        """
        Create a cluster
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Creating cluster with")
        mylog.info("\tMVIP:       " + mvip)
        mylog.info("\tSVIP:       " + svip)
        mylog.info("\tAdmin User: " + username)
        mylog.info("\tAdmin Pass: " + password)

        ntries = 0
        if node_count > 0:
            mylog.info("Waiting for " + str(node_count) + " nodes ...")
        while True:
            try:
                bootstrap = libsf.CallApiMethod(node_ip, username, password, "GetBootstrapConfig", {})
            except libsf.SfError as e:
                mylog.error("Failed to get bootstrap config: " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

            node_ips = [n["name"] for n in bootstrap["nodes"]]
            mylog.info("\tNodes:  " + ", ".join(node_ips))
            if node_count == 0 or len(bootstrap["nodes"]) >= node_count:
                break
            else:
                time.sleep(10)
            ntries += 1
            if ntries > 10:
                mylog.error("Couldn't find " + str(node_count) + " nodes, only found " + str(len(bootstrap["nodes"])))
                self.RaiseFailureEvent(message="Couldn't find " + str(node_count) + " nodes, only found " + str(len(bootstrap["nodes"])))
                return False

        mylog.info("Creating cluster ...")
        params = {}
        params["mvip"] = mvip
        params["svip"] = svip
        params["username"] = username
        params["password"] = password
        params["nodes"] = node_ips
        try:
            libsf.CallApiMethod(node_ip, username, password, "CreateCluster", params)
        except libsf.SfError as e:
            mylog.error("Failed to create cluster: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        mylog.passed("Cluster created successfully")

        if not add_drives:
            return True

        # Determine how many drives to expect in the cluster
        expected_drives = 0
        if (drive_count > 0):
            expected_drives = drive_count
        else:
            # Try to use the per-node API get the number of drives
            for node_ip in node_ips:
                try:
                    drive_config = libsf.CallNodeApiMethod(node_ip, username, password, "GetDriveConfig", {})
                except libsf.SfApiError as e:
                    if e.name == "xUnknownAPIMethod":
                        # Assume 11 drives per node
                        expected_drives += 11
                        continue
                    else:
                        mylog.error(str(e))
                        self.RaiseFailureEvent(message=str(e), exception=e)
                        return False

                expected_drives += drive_config["driveConfig"]["numBlockExpected"]
                expected_drives += drive_config["driveConfig"]["numSliceExpected"]

        mylog.info("Waiting for at least " + str(expected_drives) + " available drives...")
        # Wait a little while to make sure the MVIP is up and ready and the drives have become available
        time.sleep(20)
        actual_drives = 0
        while (actual_drives < expected_drives):
            time.sleep(10)
            try:
                drivelist = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
            except libsf.SfError as e:
                mylog.error("Failed to list drives: " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            actual_drives = len(drivelist["drives"])

        mylog.info("Adding all drives to cluster...")
        try:
            drivelist = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
        except libsf.SfError as e:
            mylog.error("Failed to list drives: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        params = {}
        params["drives"] = []
        for i in range(len(drivelist["drives"])):
            drive = drivelist["drives"][i]
            if (drive["status"].lower() == "available"):
                new_drive = dict()
                new_drive["driveID"] = drive["driveID"]
                new_drive["type"] = "automatic"
                params["drives"].append(new_drive)

        try:
            libsf.CallApiMethod(mvip, username, password, "AddDrives", params)
        except libsf.SfError as e:
            mylog.error("Failed to add drives: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        mylog.passed("All drives added")
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--node_ip", type="string", dest="node_ip", default=None, help="the IP address of the storage node to use to create the cluster")
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP of the cluster")
    parser.add_option("--svip", type="string", dest="svip", default=sfdefaults.svip, help="the storage VIP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--node_count", type="int", dest="node_count", default=0, help="total number of nodes to use to create the cluster from (default is whatever comes back from GetBootstrapConfig)")
    parser.add_option("--noadd_drives", action="store_false", dest="add_drives", default=True, help="Do not add available drives to the cluster after creation")
    parser.add_option("--drive_count", type="int", dest="drive_count", default=0, help="total number of available drives to look for after the cluster is created (default is node count * 11)")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ip, options.mvip, options.svip, options.username, options.password, options.add_drives, options.drive_count, options.node_count, options.debug):
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
