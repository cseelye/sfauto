#!/usr/bin/python

# This script will get information about a cluster.

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

enable_sc = "False"                 # --sc

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog


def main():
    global mvip, username, password

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
    parser.add_option("--sc", action="store_true", dest="enable_sc", help="collect SetConstants")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    enable_sc = options.enable_sc
    if (enable_sc == None):
        enable_sc = False
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    # Get Cluster version and volume count
    print "========= Cluster Version ========="
    result = libsf.CallApiMethod(mvip, username, password, "GetClusterVersionInfo", {})
    print "ClusterVersion: " + str(result["clusterVersion"])
    print "ClusterSize: " + str(len(result["clusterVersionInfo"]))
    nodeObj = result["clusterVersionInfo"]
    for node in nodeObj:
        print "NodeID: " + str(node["nodeID"]) + " " + "Version: " + str(node["nodeInternalRevision"])
    volResult = libsf.CallApiMethod(mvip, username, password, "ListActiveVolumes", {})
    print "NumVolumes: " + str(len(volResult["volumes"]))

    # Get Cluster info
    print "========= Cluster Info ========="
    clusterResult =  libsf.CallApiMethod(mvip, username, password, "GetClusterInfo",{})
    for key, value in clusterResult["clusterInfo"].iteritems():
        print str(key) + " = " + str(value)

    # Get capacity info
    print "========= Capacity Info ========="
    capacityResult = libsf.CallApiMethod(mvip, username, password, "GetClusterCapacity", {})
    for key, value in capacityResult["clusterCapacity"].iteritems():
        print str(key) + " = " + str(value)

    # Get node info
    print "========= Node Info ========="
    nodeResult = libsf.CallApiMethod(mvip, username, password, "ListAllNodes", {})
    nodeObj = nodeResult["nodes"]
    for node in nodeObj:
        print "Name: " + str(node["name"]) + "(" + str(node["nodeID"]) + ")" + " " + str(node["mip"])

    # Get SetConstants if needed
    if enable_sc:
        print "========= Constants ========="
        scResults =  libsf.CallApiMethod(mvip, username, password, "SetConstants", {})
        for key, value in scResults.iteritems():
            print str(key) + " = " + str(value)

    exit(0)


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
