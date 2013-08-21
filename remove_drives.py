#!/usr/bin/python

"""
This action will remove drives from the cluster from the specified nodes

When drive_slots is specified, it will remove drives from only those slots
When by_node is true, it will add the drives one node at a time instead of all at once

After drives are removed it will wait for syncing to be complete

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --node_ips          IP addresses of the nodes to remove drives from

    --by_node           Remove the drives by node vs all at once

    --drive_slots       The slot in each node to remove the drives from
"""

import sys
from optparse import OptionParser
import time
import logging
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import wait_syncing

class RemoveDrivesAction(ActionBase):
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
                            "node_ips" : lambda x: True if not x else libsf.IsValidIpv4AddressList(x),
                            #"drive_slots" : lambda x: True if not x else libsf.IsIntegerList(x)
                            },
            args)

    def Execute(self, mvip=sfdefaults.mvip, node_ips=None, by_node=False, drive_slots=None, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Remove drives from the cluster and wait for syncing
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        if drive_slots == None and node_ips == None:
            mylog.error("You need specify drive_slots or node_ips")
            return False

        # if drive_slots == None:
        #     drive_slots = []
        if node_ips == None:
            node_ips = []

        mylog.info("Getting a list of nodes/drives")
        nodeip2nodeid = dict()
        try:
            nodes_obj = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for no in nodes_obj["nodes"]:
            mip = no["mip"]
            nodeip2nodeid[mip] = no["nodeID"]

        if by_node:
            for node_ip in node_ips:
                # nodeID of this node
                node_id = nodeip2nodeid[node_ip]
                if (node_id == None):
                    mylog.error("Could not find node " + str(node_ip) + " in cluster " + str(mvip))
                    return False
                mylog.info("Removing drives from " + str(node_ip) + " (nodeID " + str(node_id) + ")")

                # make a list of drives to remove
                drives2remove = []
                try:
                    drive_list = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
                except libsf.SfError as e:
                    mylog.error("Failed to get drive list: " + str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False
                for do in drive_list["drives"]:
                    print len(drive_slots)
                    if drive_slots == None:
                        if do["status"] == "active" and str(node_id) == str(do["nodeID"]):
                            drives2remove.append(int(do["driveID"]))
                            mylog.info("  Removing driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]))
                    else:
                        if (do["status"] == "active" and str(node_id) == str(do["nodeID"])) and int(do["slot"]) in drive_slots:
                            drives2remove.append(int(do["driveID"]))
                            mylog.info("  Removing driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]))
                if drive_slots != None:

                    if len(drives2remove) != len(drive_slots):
                        mylog.error("Could not find the correct list of drives to remove (check that specified drives are active)")
                        self.RaiseFailureEvent(message="Could not find the correct list of drives to remove (check that specified drives are active)")
                        return False

                # Remove the drives
                self._RaiseEvent(self.Events.BEFORE_REMOVE)
                try:
                    libsf.CallApiMethod(mvip, username, password, "RemoveDrives", {'drives': drives2remove})
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
                    mylog.error("Failed to wait for syncing - " + str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False
                self._RaiseEvent(self.Events.AFTER_SYNC)

        else:
            for node_ip in node_ips:
                # nodeID of this node
                node_id = nodeip2nodeid[node_ip]
                if (node_id == None):
                    mylog.error("Could not find node " + str(node_ip) + " in cluster " + str(mvip))
                    self.RaiseFailureEvent(message="Could not find node " + str(node_ip) + " in cluster " + str(mvip))
                    return False
                mylog.info("Removing drives from " + str(node_ip) + " (nodeID " + str(node_id) + ")")

                # make a list of drives to remove
                drives2remove = []
                try:
                    drives_obj = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
                except libsf.SfError as e:
                    mylog.error("Failed to get drive list: " + str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False
                for do in drives_obj["drives"]:
                    if drive_slots != None:
                        if (do["status"] == "active" and str(node_id) == str(do["nodeID"])) and str(do["slot"]) in drive_slots:
                            drives2remove.append(int(do["driveID"]))
                            mylog.info("  Removing driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]))
                    else:
                        if (do["status"] == "active" and str(node_id) == str(do["nodeID"])):
                            drives2remove.append(int(do["driveID"]))
                            mylog.info("  Removing driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]))
            if drive_slots != None:
                if len(drives2remove) != len(drive_slots) * len(node_ips):
                    mylog.error("Could not find the correct list of drives to remove (check that specified drives are active)")
                    self.RaiseFailureEvent(message="Could not find the correct list of drives to remove (check that specified drives are active)")
                    return False

            # Remove the drives
            self._RaiseEvent(self.Events.BEFORE_REMOVE)
            try:
                libsf.CallApiMethod(mvip, username, password, "RemoveDrives", {'drives': drives2remove})
            except libsf.SfError as e:
                mylog.error("Failed to remove drives: " + str(e))
                return False

            self._RaiseEvent(self.Events.BEFORE_SYNC)
            mylog.info("Waiting for syncing")
            time.sleep(30)
            if wait_syncing.Execute(mvip=mvip, username=username, password=password) == False:
                mylog.error("Waiting for syncing to complete timed out")
                return False

        mylog.passed("Finished removing drives")
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
    parser.add_option("-n", "--node_ips", action="list", dest="node_ips", default=None, help="the IP addresses of the nodes to remove drives from")
    parser.add_option("--by_node", action="store_true", dest="by_node", default=False, help="add the drives by node instead of all at once")
    parser.add_option("--drive_slots", action="list", dest="drive_slots", default=None, help="the slots to add the drives from")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.node_ips, options.by_node, options.drive_slots, options.username, options.password, options.debug):
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
