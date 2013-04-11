#!/usr/bin/python

# This script will make sure the ensemble is the correct size

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

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
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)

    # Find the nodes in the cluster
    node_list = dict()
    result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
    for node in result["nodes"]:
        node_list[node["nodeID"]] = node["cip"]
    node_count = len(node_list.keys())
    mylog.info("Found " + str(node_count) + " nodes in cluster")

    # Get the ensemble list
    result = libsf.CallApiMethod(mvip, username, password, 'GetClusterInfo', {})
    mylog.info("Ensemble list: " + str(result["clusterInfo"]["ensemble"]))
    ensemble_count = len(result["clusterInfo"]["ensemble"])


    # Make sure we have the correct number of ensemble members
    if node_count < 3: # Less then 3 node, ensemble of 1
        if ensemble_count == 1:
            mylog.passed("Found " + str(ensemble_count) + " ensemble nodes")
            sys.exit(0)
        else:
            mylog.error("Found " + str(ensemble_count) + " ensemble nodes but expected 1")
            sys.exit(1)
    elif node_count < 5: # 3-4 nodes, ensemble of 3
        if ensemble_count == 3:
            mylog.passed("Found " + str(ensemble_count) + " ensemble nodes")
            sys.exit(0)
        else:
            mylog.error("Found " + str(ensemble_count) + " ensemble nodes but expected 3")
            sys.exit(1)
    else: #  5 or more nodes, ensemble of 5
        if ensemble_count == 5:
            mylog.passed("Found " + str(ensemble_count) + " ensemble nodes")
            sys.exit(0)
        else:
            mylog.error("Found " + str(ensemble_count) + " ensemble nodes but expected 5")
            sys.exit(1)


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

