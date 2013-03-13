#!/usr/bin/python

# This script will wait for there to be no active cluster faults

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

whitelist = [                       # Ignore these faults and do not wait for them to clear
    "clusterFull",                  # --whitelist
    "clusterIOPSAreOverProvisioned"
]

blacklist = [                       # Immediately fail if any of these faults are present
                                    # --blacklist
]
# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog


def main():
    global mvip, username, password, whitelist, blacklist

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password", "whitelist", "blacklist" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--whitelist", type="string", dest="whitelist", default=",".join(whitelist), help="ignore these faults and do not wait for them to clear")
    parser.add_option("--blacklist", type="string", dest="blacklist", default=",".join(blacklist), help="immediately fail if these faults are present")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    whitelist = set()
    whitelist_str = options.whitelist
    pieces = whitelist_str.split(',')
    for fault in pieces:
        fault = fault.strip()
        if fault:
            whitelist.add(fault)
    blacklist = set()
    blacklist_str = options.blacklist
    pieces = blacklist_str.split(',')
    for fault in pieces:
        fault = fault.strip()
        if fault:
            blacklist.add(fault)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)


    mylog.info("Waiting for no unresolved cluster faults on cluster " + mvip)
    if len(whitelist) > 0: mylog.info("  If these faults are present, they will be ignored: " + ", ".join(whitelist))
    if len(blacklist) > 0: mylog.info("  If these faults are present, they will cause the script to immediately fail: " + ", ".join(blacklist))

    fault_list = set()
    while True:
        result = libsf.CallApiMethod(mvip, username, password, "ListClusterFaults", {"exceptions": 1, "faultTypes": "current"})
        if len(result["faults"]) <= 0: break
        previous_fault_list = fault_list
        fault_list = set()
        for fault in result["faults"]:
            if fault["code"] not in fault_list:
                fault_list.add(fault["code"])

        # Break if the only current faults are ignored faults
        if fault_list & whitelist == fault_list: break

        # Print the list of faults if it is the first time or if it has changed
        if previous_fault_list == set() or fault_list & previous_fault_list != previous_fault_list:
            mylog.info("   Current faults: " + ",".join(fault_list))

        # Abort if there are any blacklisted faults
        if len(fault_list & blacklist) > 0:
            mylog.error("Blacklisted fault found")
            sys.exit(1)

        time.sleep(60)

    mylog.passed("There are no current cluster faults on " + mvip)


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







