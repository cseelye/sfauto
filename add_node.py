#!/usr/bin/python

# This script will add a node to a cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

node_ip = "192.168.000.000"         # The management IP of the node to add
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
    parser.add_option("--node_ip", type="string", dest="node_ip", default=node_ip, help="the mIP of the node to add")
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
        mylog.error("'" + node_ip + "' does not appear to be a valid node IP")
        sys.exit(1)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)


    # Find the nodeID of the requested node
    mylog.info("Searching for nodes")
    node_id = 0
    result = libsf.CallApiMethod(mvip, username, password, "ListPendingNodes", {})
    for node in result["pendingNodes"]:
        if node["mip"] == node_ip:
            node_id = node["pendingNodeID"]
            break
    if node_id <= 0:
        mylog.error("Could not find node " + node_ip)
        sys.exit(1)
    mylog.info("Found node " + node_ip + " is nodeID " + str(node_id))

    # Add the node
    mylog.info("Adding " + node_ip + " to cluster")
    result = libsf.CallApiMethod(mvip, username, password, "AddNodes", {"pendingNodes" : [node_id]})
    time.sleep(20)

    mylog.passed("Successfully added " + node_ip + " to cluster")



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

