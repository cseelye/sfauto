#!/usr/bin/python

# This script will check that a cluster is healthy

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

ssh_user = "root"                   # The SSH username for the nodes
                                    # --ssh_user

ssh_pass = "password"              # The SSH password for the nodes
                                    # --ssh_pass

mvip = "192.168.0.0"                # The management VIP for the cluster
                                    # --mvip

username = "admin"                  # The username for the nodes
                                    # --user

password = "password"              # The password for the nodes
                                    # --pass

since = 0                           # Timestamp of when to check health from.  events, cores, etc from before this will be ignored
                                    # --since

fault_whitelist = [                 # Ignore these faults if they are present
    "clusterFull",                  # --whitelist
    "clusterIOPSAreOverProvisioned",
    "nodeHardwareFault"
]

ignore_faults = False               # Do not check for cluster faults
                                    # --ignore_faults

ignore_cores = False                # Do not check for core files
                                    # --ignore_coress

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import libsf
from libsf import mylog

def main():
    global mvip, ssh_user, ssh_pass, username, password, since, fault_whitelist, ignore_cores, ignore_faults

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management VIP for the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the username for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the password for the cluster")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=ssh_user, help="the SSH username for the nodes")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=ssh_pass, help="the SSH password for the nodes")
    parser.add_option("--since", type="int", dest="since", default=since, help="timestamp of when to check health from")
    parser.add_option("--fault_whitelist", type="string", dest="fault_whitelist", default=",".join(fault_whitelist), help="ignore these faults and do not wait for them to clear")
    parser.add_option("--ignore_cores", action="store_true", dest="ignore_cores", help="ignore core files on nodes")
    parser.add_option("--ignore_faults", action="store_true", dest="ignore_faults", help="ignore cluster faults")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    username = options.username
    password = options.password
    mvip = options.mvip
    since = options.since
    if options.ignore_cores: ignore_cores = True
    if options.ignore_faults: ignore_faults = True
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)
    fault_whitelist = set()
    whitelist_str = options.fault_whitelist
    pieces = whitelist_str.split(',')
    for fault in pieces:
        fault = fault.strip()
        if fault:
            fault_whitelist.add(fault)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)


    # get the list of nodes in the cluster
    node_ips = []
    obj = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
    for node in obj["nodes"]:
        node_ips.append(node["mip"])
    node_ips.sort()

    healthy = True

    # Check for core files
    if not ignore_cores:
        for node_ip in node_ips:
            mylog.info("Checking for core files on " + node_ip)
            core_count = libsf.CheckCoreFiles(node_ip, ssh_user, ssh_pass, since)
            if (core_count > 0):
                healthy = False
                mylog.error("Found " + str(core_count) + " core files on " + node_ip)

    # Check for xUnknownBlockID
    mylog.info("Checking for errors in cluster event log")
    if libsf.CheckForEvent("xUnknownBlockID", mvip, username, password, since):
        healthy = False
        mylog.error("Found xUnknownBlockId in the event log")

    # Check current cluster faults
    if not ignore_faults:
        mylog.info("Checking for unresolved cluster faults")
        if len(fault_whitelist) > 0: mylog.info("  If these faults are present, they will be ignored: " + ", ".join(fault_whitelist))
        obj = libsf.CallApiMethod(mvip, username, password, "ListClusterFaults", {"exceptions": 1, "faultTypes": "current"})
        if (len(obj["faults"]) > 0):
            current_faults = set()
            for fault in obj["faults"]:
                if fault["code"] not in current_faults:
                    current_faults.add(fault["code"])

            if current_faults & fault_whitelist == current_faults:
                mylog.info("Current cluster faults found: " + ", ".join(current_faults))
            else:
                healthy = False
                mylog.error("Current cluster faults found: " + ", ".join(current_faults))


    if not healthy:
        mylog.error("Cluster is not healthy")
        exit(1)
    else:
        mylog.passed("Cluster is healthy")
        exit(0)


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
