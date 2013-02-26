#!/usr/bin/python

# This script will print a random node mIP from the cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

ensemble = False                    # Only select from ensemble nodes
                                    # --ensemble

nomaster = False                   # Do not select the cluster master
                                    # --nomaster

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
from random import randint
import libsf
from libsf import mylog


def main():
    global mvip, username, password, ensemble, nomaster

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
    parser.add_option("--ensemble", action="store_true", dest="ensemble", help="only select from ensemble nodes")
    parser.add_option("--nomaster", action="store_true", dest="nomaster", help="do not select the cluster master")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    if options.ensemble:
        ensemble = True
    if options.nomaster:
        nomaster = True
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)


    master_id = None
    if nomaster:
        result = libsf.CallApiMethod(mvip, username, password, 'GetClusterMasterNodeID', {})
        master_id = result["nodeID"]
        #mylog.debug("Cluster master is nodeID " + str(master_id))

    node_list = []
    if ensemble:
        # Find the nodes in the cluster
        node_ref = dict()
        result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
        for node in result["nodes"]:
            if node["nodeID"] == master_id:
                mylog.debug("Cluster master is " + node["mip"])
                continue
            node_ref[node["cip"]] = node["mip"]

        # Get the ensemble list
        result = libsf.CallApiMethod(mvip, username, password, 'GetClusterInfo', {})
        for node_cip in result["clusterInfo"]["ensemble"]:
            if node_cip in node_ref:
                node_list.append(node_ref[node_cip])
        node_count = len(node_list)
        node_list.sort()
        mylog.debug("Found " + str(node_count) + " eligible nodes in cluster " + mvip + ": " + str(node_list))
    else:
        # Find the nodes in the cluster
        result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
        for node in result["nodes"]:
            if node["nodeID"] == master_id:
                mylog.debug("Cluster master is " + node["mip"])
                continue
            node_list.append(node["mip"])
        node_count = len(node_list)
        node_list.sort()
        mylog.debug("Found " + str(node_count) + " eligible nodes in cluster " + mvip + ": " + str(node_list))

    index = randint(0, len(node_list)-1)
    sys.stdout.write(node_list[index] + "\n")
    sys.stdout.flush()
    sys.exit(0)


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

