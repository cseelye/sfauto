#!/usr/bin/python

# This script will set the clustername of a pending node

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

node_ip = "192.168.000.000"         # The management IP of the node
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "solidfire"              # Admin password for the cluster
                                    # --pass

cluster_name = ""                   # The new cluster for the node

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog


def main():
    global node_ip, username, password, hostname

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "username", "password" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--node_ip", type="string", dest="node_ip", default=node_ip, help="the management IP of the node")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--cluster_name", type="string", dest="cluster_name", default=cluster_name, help="the new cluster for the node")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    node_ip = options.node_ip
    username = options.username
    password = options.password
    cluster_name = options.cluster_name
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(node_ip):
        mylog.error("'" + node_ip + "' does not appear to be a valid node IP")
        sys.exit(1)

    mylog.info("Setting cluster to '" + cluster_name + "' on node " + str(node_ip))
    params = {}
    params["cluster"] = {}
    params["cluster"]["cluster"] = cluster_name
    result = libsf.CallNodeApiMethod(node_ip, username, password, "SetConfig", params)

    mylog.passed("Successfully set hostname")

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

