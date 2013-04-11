#!/usr/bin/python

# This script will create a cluster, wait for all of the drives to to available and then add them

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"        # The management VIP of the cluster
                                # --mvip

svip = "10.10.000.000"          # The storage VIP of the cluster
                                # --svip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass

node_ip = "192.168.133.000"     # The IP address of the storage node to use to create the cluster
                                # --node_ip

drive_count = 0                 # Override how many available drives to look for
                                # --drive_count

node_count = 0                  # The required number of nodes to create the cluster with.
                                # if zero, use whatever comes back from the first call to GetBootstrapConfig
                                # --node_count

# ----------------------------------------------------------------------------

import sys, os
import os
from optparse import OptionParser
import json
import urllib2
import random
import re
import platform
import time
import libsf
from libsf import mylog

def main():
    global mvip, svip, username, password, node_ip, drive_count, node_count

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "svip", "username", "password" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management VIP of the cluster")
    parser.add_option("--svip", type="string", dest="svip", default=svip, help="the storage VIP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--node_ip", type="string", dest="node_ip", default=node_ip, help="the IP address of the storage node to use to create the cluster")
    parser.add_option("--drive_count", type="int", dest="drive_count", default=drive_count, help="total number of available drives to look for after the cluster is created (default is node count * 11)")
    parser.add_option("--node_count", type="int", dest="node_count", default=node_count, help="total number of nodes to use to create the cluster from (default is whatever comes back from GetBootstrapConfig)")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    svip = options.svip
    username = options.username
    password = options.password
    node_ip = options.node_ip
    drive_count = options.drive_count
    node_count = options.node_count
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)
    if not libsf.IsValidIpv4Address(svip):
        mylog.error("'" + svip + "' does not appear to be a valid SVIP")
        sys.exit(1)


    mylog.info("Creating cluster with")
    mylog.info("\tMVIP:       " + mvip)
    mylog.info("\tSVIP:       " + svip)
    mylog.info("\tAdmin User: " + username)
    mylog.info("\tAdmin Pass: " + password)


    params = {}
    ntries = 0
    mylog.info("Waiting for " + str(node_count) + " nodes ...")
    while True:
        response = libsf.CallApiMethod(node_ip, username, password, "GetBootstrapConfig", params)
        nodelist = response["nodes"]
        mylog.info("\tNodes:      " + str(nodelist))
        if node_count == 0 or len(nodelist) >= node_count:
            break
        else:
            time.sleep(5)
        ntries += 1
        if ntries > 10:
            mylog.error("couldn't find " + str(node_count) + " nodes, only found " + str(len(nodelist)))
            sys.exit(1)

    mylog.info("Creating cluster ...")
    params = {}
    params["mvip"] = mvip
    params["svip"] = svip
    params["username"] = username
    params["password"] = password
    params["nodes"] = nodelist
    response = libsf.CallApiMethod(node_ip, username, password, "CreateCluster", params)
    mylog.passed("Cluster created successfully")

    expected_drives = len(nodelist) * 11
    if (drive_count > 0):
        expected_drives = drive_count
    mylog.info("Waiting for " + str(expected_drives) + " available drives...")
    actual_drives = 0

    # Starting with Be, the MVIP usually isn't ready right after the create call returns; need to wait a few seconds before trying any API calls
    time.sleep(20)

    while (actual_drives < expected_drives):
        params = {}
        response = libsf.CallApiMethod(mvip, username, password, "ListDrives", params)
        actual_drives = len(response["drives"])

    mylog.info("Adding all drives to cluster...")
    drive_list = []
    for i in range(len(response["drives"])):
        drive = response["drives"][i]
        if (drive["status"].lower() == "available"):
            new_drive = dict()
            new_drive["driveID"] = drive["driveID"]
            new_drive["type"] = "automatic"
            drive_list.append(new_drive)
    params = {}
    params["drives"] = drive_list
    response = libsf.CallApiMethod(mvip, username, password, "AddDrives", params)
    mylog.passed("All drives added")


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        timer = libsf.ScriptTimer()
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



