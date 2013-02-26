#!/usr/bin/env python

# This script waits for a given number of available drives to be in the cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.0.0"        # The management VIP for the cluster
                            # --mvip

username = "admin"          # The cluster username
                            # --user

password = "password"      # The cluster password
                            # --pass

drive_count = 0             # The number of drives to wait for
                            # --drive_count

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog

def main():
    global mvip, username, password, drive_count

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line options
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management VIP for the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the username for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the password for the cluster")
    parser.add_option("--drive_count", type="int", dest="drive_count", default=drive_count, help="the number of drives to wait for")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    username = options.username
    password = options.password
    mvip = options.mvip
    drive_count = options.drive_count
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    mylog.info("Waiting for " + str(drive_count) + " available drives...")
    while True:
        avail_count = 0
        params = dict()
        params["drives"] = []
        result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
        for drive in result["drives"]:
            if drive["status"] == "available":
                avail_count += 1
        if avail_count >= drive_count:
            mylog.passed("Found " + str(avail_count) + " available drives")
            sys.exit(0)

        mylog.info("Found " + str(avail_count) + " available drives")
        time.sleep(10)


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

