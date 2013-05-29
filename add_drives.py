#!/usr/bin/python

"""
This action will add available drives to the cluster from one or more nodes

When node_ips is specifed, it will add available drives from those nodes
When drive_slots is specified, it will add available drives from those slots
The two options can be combined to add avaialble drives that are in particular slots in particular nodes

When by_node is true, it will add the drives one node at a time instead of all at once

After drives are added it will wait for syncing to be complete

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --node_ips          IP addresses of the nodes to add drives from

    --by_node           Add the drives by node vs all at once

    --drive_slots       The slot in each node to add the drives from
"""

import sys
from optparse import OptionParser
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import logging
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class AddDrivesAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_ADD = "BEFORE_ADD"
        AFTER_ADD = "AFTER_ADD"
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
                            "drive_slots" : lambda x: True if not x else libsf.IsIntegerList(x)
                            },
            args)

    def Execute(self, mvip=sfdefaults.mvip, node_ips=None, by_node=False, drive_slots=None, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Add available drives to the cluster, by node and/or slot, and wait for syncing
        """
        if drive_slots == None:
            drive_slots = []
        if node_ips == None:
            node_ips = []
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Getting a list of nodes/drives")
        nodeip2nodeid = dict()
        node_ids = []
        try:
            nodes_obj = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        for no in nodes_obj["nodes"]:
            mip = no["mip"]
            if not node_ips or mip in node_ips:
                nid = no["nodeID"]
                nodeip2nodeid[mip] = nid
                node_ids.append(nid)
        if (node_ips == None or len(node_ips) <= 0):
            node_ips = nodeip2nodeid.keys()

        if by_node:
            for node_ip in node_ips:
                # nodeID of this node
                node_id = nodeip2nodeid[node_ip]
                if (node_id == None):
                    mylog.error("Could not find node " + str(node_ip) + " in cluster " + str(mvip))
                    return False
                mylog.info("Adding drives from " + str(node_ip) + " (nodeID " + str(node_id) + ")")

                # make a list of drives to add
                mylog.info("Looking for drives...")
                drives2add = []
                try:
                    drives_obj = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
                except libsf.SfError as e:
                    mylog.error("Failed to get drive list: " + str(e))
                    return False
                for do in drives_obj["drives"]:
                    if do["status"].lower() != "available": continue

                    if drive_slots == None or len(drive_slots) <= 0:
                        mylog.info("  Found available driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]))
                        drive = dict()
                        drive["driveID"] = int(do["driveID"])
                        drive["type"] = "automatic"
                        drives2add.append(drive)
                    elif str(node_id) == str(do["nodeID"]) and int(do["slot"]) in drive_slots:
                        mylog.info("  Found available driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]))
                        drive = dict()
                        drive["driveID"] = int(do["driveID"])
                        drive["type"] = "automatic"
                        drives2add.append(drive)
                if len(drive_slots) > 0 and len(drives2add) != len(drive_slots):
                    mylog.error("Could not find the expected list of drives to add (check that specified drives are available)")
                    return False
                if len(drives2add) <= 0:
                    mylog.info("No available drives to add")
                    return True

                # Add the drives
                self._RaiseEvent(self.Events.BEFORE_ADD)
                try:
                    libsf.CallApiMethod(mvip, username, password, "AddDrives", {'drives': drives2add})
                except libsf.SfError as e:
                    mylog.error("Failed to add drives: " + str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False

                self._RaiseEvent(self.Events.BEFORE_SYNC)
                mylog.info("Waiting for syncing...")
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
            # make a list of drives to add
            mylog.info("Looking for drives...")
            drives2add = []
            try:
                drives_obj = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
            except libsf.SfError as e:
                mylog.error("Failed to get drive list: " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            for do in drives_obj["drives"]:
                if do["status"].lower() != "available": continue
                #mylog.debug("Found d" + str(do["driveID"]) + " s" + str(do["slot"]) + " n" + str(do["nodeID"]))
                if (drive_slots == None or len(drive_slots) <= 0) and (node_ids == None or len(node_ids) <= 0):
                    mylog.info("  Found available driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]) + " in node " + str(do["nodeID"]))
                    drive = dict()
                    drive["driveID"] = int(do["driveID"])
                    drive["type"] = "automatic"
                    drives2add.append(drive)
                elif (drive_slots == None or len(drive_slots) <= 0) and do["nodeID"] in node_ids:
                    mylog.info("  Found available driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]) + " in node " + str(do["nodeID"]))
                    drive = dict()
                    drive["driveID"] = int(do["driveID"])
                    drive["type"] = "automatic"
                    drives2add.append(drive)
                elif (node_ids == None or len(node_ids) <= 0) and do["nodeID"] in node_ids:
                    mylog.info("  Found available driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]) + " in node " + str(do["nodeID"]))
                    drive = dict()
                    drive["driveID"] = int(do["driveID"])
                    drive["type"] = "automatic"
                    drives2add.append(drive)
                elif do["nodeID"] in node_ids and int(do["slot"]) in drive_slots:
                    mylog.info("  Found available driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]) + " in node " + str(do["nodeID"]))
                    drive = dict()
                    drive["driveID"] = int(do["driveID"])
                    drive["type"] = "automatic"
                    drives2add.append(drive)

            if len(drive_slots) > 0 and len(drives2add) != len(drive_slots):
                mylog.error("Could not find the expected list of drives to add (check that specified drives are available)")
                return False
            if len(drives2add) <= 0:
                mylog.info("No available drives to add")
                return True

            # Add the drives
            self._RaiseEvent(self.Events.BEFORE_ADD)
            try:
                libsf.CallApiMethod(mvip, username, password, "AddDrives", {'drives': drives2add})
            except libsf.SfError as e:
                mylog.error("Failed to add drives: " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

            self._RaiseEvent(self.Events.BEFORE_SYNC)
            mylog.info("Waiting for syncing...")
            time.sleep(120)
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

        mylog.passed("Finished adding drives")
        self._RaiseEvent(self.Events.AFTER_ADD)
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
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)
    sys.exit(0)
