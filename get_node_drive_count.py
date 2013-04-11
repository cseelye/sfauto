#!/usr/bin/python

# This script will count the number of drives in a node

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

node_ip = "192.168.000.000"         # The management IP of the node to with the drives to count
                                    # --node_ip

csv = False                     # Display minimal output that is suitable for piping into other programs
                                # --csv

bash = False                    # Display minimal output that is formatted for a bash array/for  loop
                                # --bash

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog


def main():
    global mvip, username, password, node_ip, csv, bash

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
    parser.add_option("--csv", action="store_true", dest="csv", help="display a minimal output that is suitable for piping into other programs")
    parser.add_option("--bash", action="store_true", dest="bash", help="display a minimal output that is formatted for a bash array/for loop")
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
    if options.csv:
        csv = True
        mylog.silence = True
    if options.bash:
        bash = True
        mylog.silence = True
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)


    # Find the nodeID of the requested node
    mylog.info("Searching for node")
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

    # Count all of the drives (active, failed, vailable) in the node
    mylog.info("Searching for drives")
    drive_count = 0
    result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
    for drive in result["drives"]:
        if drive["nodeID"] == node_id:
            drive_count += 1

    if csv or bash:
        sys.stdout.write(str(drive_count) + "\n")
        sys.stdout.flush()
    else:
        mylog.info("There are " + str(drive_count) + " drives in node " + node_ip)




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

