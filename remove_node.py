#!/usr/bin/python

# This script will remove a node from a cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

node_ip = "192.168.000.000"         # The management IP of the node to remove
                                    # --node_ip

remove_drives = False               # Remove all drives in the node before removing the node
                                    # --remove_drives

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog, SfError, SfApiError


def main():
    global mvip, username, password, node_ip, remove_drives

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
    parser.add_option("--node_ip", type="string", dest="node_ip", default=node_ip, help="the mIP of the node to remove")
    parser.add_option("--remove_drives", action="store_true", dest="remove_drives", help="remove all active/failed drives from node first")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    node_ip = options.node_ip
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)
    if not libsf.IsValidIpv4Address(node_ip):
        mylog.error("'" + node_ip + "' does not appear to be a node IP")
        sys.exit(1)
    if options.remove_drives:
        remove_drives = True
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)


    # Find the nodeID of the requested node
    mylog.info("Searching for nodes")
    node_id = 0
    result = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
    for node in result["nodes"]:
        if node["mip"] == node_ip:
            node_id = node["nodeID"]
            break
    if node_id <= 0:
        mylog.error("Could not find node " + node_ip)
        sys.exit(1)
    mylog.info("Found node " + node_ip + " is nodeID " + str(node_id))

    # Remove all active and failed drives from the node
    if remove_drives:
        mylog.info("Searching for drives in node " + node_ip)
        drives_to_remove = []
        result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
        for drive in result["drives"]:
            if (drive["status"].lower() == "active" or drive["status"].lower() == "active") and node_id == drive["nodeID"]:
                drives_to_remove.append(drive["driveID"])
        if len(drives_to_remove) > 0:
            mylog.info("Removing " + str(len(drives_to_remove)) + " drives " + str(drives_to_remove))
            remove_time = time.time()
            time.sleep(2)
            libsf.CallApiMethod(mvip, username, password, "RemoveDrives", {'drives': drives_to_remove})

            # Make sure bin sync is done
            libsf.WaitForBinSync(mvip, username, password, remove_time)

            # Wait for no faults
            mylog.info("Waiting for all cluster faults to clear")
            while True:
                result = libsf.CallApiMethod(mvip, username, password, "ListClusterFaults", {'faultTypes' : 'current'})
                done = True
                for fault in result["faults"]:
                    if "unhealthy" in fault["code"].lower() or "degraded" in fault["code"].lower():
                        done = False
                        break
                if done: break
                time.sleep(60)

    # Remove the node
    while True:
        mylog.info("Removing " + node_ip + " from cluster")
        try:
            libsf.CallApiMethod(mvip, username, password, "RemoveNodes", {"nodes" : [node_id]}, ExitOnError=False)
            break
        except SfApiError as e:
            if e.name == "xDBConnectionLoss":
                # Often happens when removing ensemble members
                mylog.warning("xDBConnectionLoss - making sure node was actually removed")
                # Just retry and catch xNodeIDDoesNotExist fault if it was already removed
                time.sleep(5)
                continue
                #result = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
                #node_found = False
                #for node in result["nodes"]:
                #    if node["mip"] == node_ip:
                #        node_found = True
                #        break
                #if not node_found: break
            elif e.name == "xNodeIDDoesNotExist":
                # Node was actually removed, but an error happened responding to the API call and the retry logic triggered this
                break
            else:
                mylog.error("Error " + e.name + " - " + e.message)
                sys.exit(1)

    mylog.passed("Successfully removed " + node_ip + " from cluster")


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

