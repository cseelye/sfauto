#!/usr/bin/python

# This script will remove all the drives in a node from a cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

node_ip = "192.168.000.000"         # The management IP of the node to with the drives to remove
                                    # --node_ip

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog


def main():
    global mvip, username, password, node_ip

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--node_ip", type="string", dest="node_ip", default=node_ip, help="the management IP(s) of the node(s) to remove")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    try:
        node_ips = libsf.ParseIpsFromList(options.node_ip)
    except TypeError as e:
        mylog.error(e)
        sys.exit(1)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)


    # Find the nodeID of the requested node
    mylog.info("Searching for nodes")
    node_ids = []
    result = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
    for node_ip in node_ips:
        found = False
        for node in result["nodes"]:
            if node["mip"] == node_ip:
                node_ids.append(node["nodeID"])
                found = True
                break
        if not found:
            mylog.error("Could not find node " + node_ip + " in cluster " + mvip)
            sys.exit(1)

    # Remove all active and failed drives from the nodes
    mylog.info("Searching for drives")
    drives_to_remove = []
    result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
    for drive in result["drives"]:
        if (drive["status"].lower() == "active" or drive["status"].lower() == "failed") and drive["nodeID"] in node_ids:
            drives_to_remove.append(drive["driveID"])
    if len(drives_to_remove) > 0:
        mylog.info("Removing " + str(len(drives_to_remove)) + " drives " + str(drives_to_remove))
        libsf.CallApiMethod(mvip, username, password, "RemoveDrives", {'drives': drives_to_remove})

        mylog.info("Waiting for syncing")
        time.sleep(60)
        # Wait for bin syncing
        while libsf.ClusterIsBinSyncing(mvip, username, password):
            time.sleep(30)
        # Wait for slice syncing
        while libsf.ClusterIsSliceSyncing(mvip, username, password):
            time.sleep(30)
    else:
        mylog.info("Found no drives to remove")

    mylog.passed("Successfully removed drives from node(s)")






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

