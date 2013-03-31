#!/usr/bin/python

# This script adds one or more drives from one or more nodes

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass

node_ips = [                    # The IP addresses of the storage nodes to add the drives from
    #"192.168.133.0"            # --node_ips

]

by_node = False                 # Add the drives by node or all at once
                                # --by_node

drive_slots = [                 # The slot to add the drive from
    #1,                         # --drive_slots
]

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import json
import urllib2
import random
import re
import platform
import time
import datetime
import libsf
from libsf import mylog


def main():
    global mvip, username, password, node_ips, drive_slots, wait_threshold, email_notify

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password", "email_notify" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management VIP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--node_ips", type="string", dest="node_ips", default=",".join(node_ips), help="the IP addresses of the nodes to add drives from")
    parser.add_option("--by_node", action="store_true", dest="by_node", help="add the drives by node instead of all at once")
    parser.add_option("--drive_slots", type="string", dest="drive_slots", default=",".join(drive_slots), help="the slots to add the drives from")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    by_node = options.by_node
    wait_threshold = options.wait_threshold
    email_notify = options.email_notify
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)
    drive_slots = []
    drive_slots_str = options.drive_slots
    pieces = drive_slots_str.split(',')
    for slot in pieces:
        try:
            slot = int(slot.strip())
        except ValueError:
            mylog.error("'" + slot + "' does not appear to be a valid slot number")
            sys.exit(1)
        if slot < -1 or slot > 10:
            mylog.error("'" + slot + "' does not appear to be a valid slot number")
            sys.exit(1)
        drive_slots.append(slot)
    try:
        node_ips = libsf.ParseIpsFromList(options.node_ips)
    except TypeError as e:
        mylog.error(e)
        sys.exit(1)
    if not node_ips:
        mylog.error("Please supply at least one node IP address")
        sys.exit(1)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)

    #if (len(node_ips) <= 0):
    #    print "Please supply at least one node IP address (--node_ips \"x.x.x.x, y.y.y.y\")"
    #    exit()

    mylog.info("Getting a list of nodes/drives")
    #nodename2nodeid = dict()
    nodeip2nodeid = dict()
    node_ids = []
    nodes_obj = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
    for no in nodes_obj["nodes"]:
        mip = no["mip"]
        if mip in node_ips:
            id = no["nodeID"]
            #name = no["name"]
            #nodename2nodeid[name] = id
            nodeip2nodeid[mip] = id
            node_ids.append(id)

    if by_node:
        if (node_ips == None or len(node_ips) <= 0):
            node_ips = nodeip2nodeid.keys()

        for node_ip in node_ips:
            # nodeID of this node
            node_id = nodeip2nodeid[node_ip]
            if (node_id == None):
                mylog.error("Could not find node " + str(node_ip) + " in cluster " + str(mvip))
                exit(1)
            mylog.info("Adding drives from " + str(node_ip) + " (nodeID " + str(node_id) + ")")

            # make a list of drives to add
            drives2add = []
            drives_obj = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
            for do in drives_obj["drives"]:
                if do["status"].lower() != "available": continue

                if drive_slots == None or len(drive_slots) <= 0:
                    mylog.info("  Adding driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]))
                    drive = dict()
                    drive["driveID"] = int(do["driveID"])
                    drive["type"] = "automatic"
                    drives2add.append(drive)
                elif str(node_id) == str(do["nodeID"]) and int(do["slot"]) in drive_slots:
                    mylog.info("  Adding driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]))
                    drive = dict()
                    drive["driveID"] = int(do["driveID"])
                    drive["type"] = "automatic"
                    drives2add.append(drive)
            if len(drive_slots) > 0 and len(drives2add) != len(drive_slots):
                mylog.error("Could not find the expected list of drives to add (check that specified drives are available)")
                exit(1)
            if len(drives2add) <= 0:
                mylog.info("No available drives to add")
                exit(0)

            # Add the drives
            libsf.CallApiMethod(mvip, username, password, "AddDrives", {'drives': drives2add})

            mylog.info("Waiting for syncing")
            time.sleep(60)
            # Wait for bin syncing
            while libsf.ClusterIsBinSyncing(mvip, username, password):
                time.sleep(30)
            # Wait for slice syncing
            while libsf.ClusterIsSliceSyncing(mvip, username, password):
                time.sleep(30)

    else:
        if len(node_ips) > 1:
            mylog.info("Adding drives from all nodes at once")
        # make a list of drives to add
        drives2add = []
        drives_obj = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
        for do in drives_obj["drives"]:
            if do["status"].lower() != "available": continue
            #print "Found d" + str(do["driveID"]) + " s" + str(do["slot"]) + " n" + str(do["nodeID"])
            if (drive_slots == None or len(drive_slots) <= 0) and (node_ids == None or len(node_ids) <= 0):
                mylog.info("  Adding driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]) + " in node " + str(do["nodeID"]))
                drive = dict()
                drive["driveID"] = int(do["driveID"])
                drive["type"] = "automatic"
                drives2add.append(drive)
            elif (drive_slots == None or len(drive_slots) <= 0) and do["nodeID"] in node_ids:
                mylog.info("  Adding driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]) + " in node " + str(do["nodeID"]))
                drive = dict()
                drive["driveID"] = int(do["driveID"])
                drive["type"] = "automatic"
                drives2add.append(drive)
            elif (node_ids == None or len(node_ids) <= 0) and do["nodeID"] in node_ids:
                mylog.info("  Adding driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]) + " in node " + str(do["nodeID"]))
                drive = dict()
                drive["driveID"] = int(do["driveID"])
                drive["type"] = "automatic"
                drives2add.append(drive)
            elif do["nodeID"] in node_ids and int(do["slot"]) in drive_slots:
                mylog.info("  Adding driveID " + str(do["driveID"]) + " from slot " + str(do["slot"]) + " in node " + str(do["nodeID"]))
                drive = dict()
                drive["driveID"] = int(do["driveID"])
                drive["type"] = "automatic"
                drives2add.append(drive)

        if len(drive_slots) > 0 and len(drives2add) != len(drive_slots):
            mylog.error("Could not find the expected list of drives to add (check that specified drives are available)")
            exit(1)
        if len(drives2add) <= 0:
            mylog.info("No available drives to add")
            exit(0)

        # Add the drives
        libsf.CallApiMethod(mvip, username, password, "AddDrives", {'drives': drives2add})

        mylog.info("Waiting for syncing")
        time.sleep(60)
        # Wait for bin syncing
        while libsf.ClusterIsBinSyncing(mvip, username, password):
            time.sleep(30)
        # Wait for slice syncing
        while libsf.ClusterIsSliceSyncing(mvip, username, password):
            time.sleep(30)

    mylog.passed("Finished adding drives")

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)
