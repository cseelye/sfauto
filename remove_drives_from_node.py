#!/usr/bin/python

"""
This action will remove all of the drives from the specified node

After drives are removed it will wait for syncing to be complete

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --node_ips          IP addresses of the nodes to remove drives from
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

class RemoveDrivesFromNodeAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_REMOVE = "BEFORE_REMOVE"
        AFTER_REMOVE = "AFTER_REMOVE"
        BEFORE_SYNC = "BEFORE_SYNC"
        AFTER_SYNC = "AFTER_SYNC"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "node_ips" : libsf.IsValidIpv4AddressList,
                            },
            args)

    def Execute(self, node_ips, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Remove drives from the nodes
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster = libsfcluster.SFCluster(mvip, username, password)

        # Find the requested nodes
        mylog.info("Searching for nodes")
        node_list = []
        for node_ip in node_ips:
            try:
                node = cluster.GetNode(node_ip)
            except libsf.SfUnknownObjectError:
                mylog.error("Could not find node " + node_ip)
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            except libsf.SfError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            node_list.append(node)

        # Find all of the active drives in the nodes
        mylog.info("Looking for drives")
        drive_list = []
        for node in node_list:
            node_drives = node.ListDrives()
            for drive in node_drives:
                if drive["status"].lower() == "active" or drive["status"].lower() == "failed":
                    mylog.info("  Found driveID " + str(drive["driveID"]) + " from slot " + str(drive["slot"]))
                    drive_list.append(drive["driveID"])

        # Remove the drives from the cluster
        self._RaiseEvent(self.Events.BEFORE_REMOVE)
        if len(drive_list) > 0:
            mylog.info("Removing " + str(len(drive_list)) + " drives")
            try:
                libsf.CallApiMethod(mvip, username, password, "RemoveDrives", {'drives': drive_list})
            except libsf.SfError as e:
                mylog.error("Failed to remove drives: " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

            self._RaiseEvent(self.Events.BEFORE_SYNC)
            mylog.info("Waiting for syncing")
            time.sleep(60)
            try:
                # Wait for bin syncing
                while libsf.ClusterIsBinSyncing(mvip, username, password):
                    time.sleep(30)
                # Wait for slice syncing
                while libsf.ClusterIsSliceSyncing(mvip, username, password):
                    time.sleep(30)
            except libsf.SfError as e:
                mylog.error("Failed to wait for syncing: " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            self._RaiseEvent(self.Events.AFTER_SYNC)

        else:
            mylog.info("Found no drives to remove")

        mylog.passed("Successfully removed drives")
        self._RaiseEvent(self.Events.AFTER_REMOVE)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("-n", "--node_ips", action="list", dest="node_ips", default=None, help="the IP addresses of the nodes to add drives from")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ips, options.mvip, options.username, options.password, options.debug):
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

