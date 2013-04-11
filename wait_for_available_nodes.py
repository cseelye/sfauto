#!/usr/bin/python

# This script will wait for nodes to be in the avaialble nodes list

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

node_ips = [
    "192.168.000.000"               # The management IPs of the nodes to wait for
]                                   # --node_ips

timeout = 300                       # How long to wait (in sec)
                                    # --timeout

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog


def main():
    global mvip, username, password, node_ips, timeout

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
    parser.add_option("--node_ips", type="string", dest="node_ips", default=",".join(node_ips), help="the IP addresses of the nodes")
    parser.add_option("--timeout", type="int", dest="timeout", default=timeout, help="how long to wait (in sec)")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    timeout = options.timeout
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)
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
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)


    mylog.info("Waiting " + str(timeout) + " sec for nodes " + str(node_ips) + " to be available in cluster " + mvip)
    start_time = time.time()
    while True:
        # Make a list of the available nodes
        avail_nodes = set()
        result = libsf.CallApiMethod(mvip, username, password, "ListPendingNodes", {})
        for node in result["pendingNodes"]:
            avail_nodes.add(node["mip"])

        mylog.debug("Pending node list: " + str(avail_nodes))

        # Check that the requested nodes are all present
        all_found = True
        for node_ip in node_ips:
            if node_ip not in avail_nodes:
                all_found = False
                break

        if all_found: break
        time.sleep(10)
        if time.time() - start_time > timeout:
            mylog.error("Did not find node " + node_ip + " in pending list")
            sys.exit(1)

    mylog.passed("All requested nodes are available")



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

