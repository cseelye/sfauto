#!/usr/bin/env python

# This script will find all drives in the available pool and add them to the cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.0.0"        # The management VIP for the cluster
                            # --mvip

username = "admin"          # The cluster username
                            # --user

password = "password"      # The cluster password
                            # --pass

no_sync = False             # Do not wait for syncing after adding drives.
                            # --no_sync

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog
import json

def main():
    global mvip, username, password, no_sync

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
    parser.add_option("--no_sync", action="store_true", dest="no_sync", help="do not wait for syncing after adding the drives")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    username = options.username
    password = options.password
    mvip = options.mvip
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)
    if options.no_sync:
        no_sync = True
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)

    mylog.info("Searching for available drives...")
    params = dict()
    params["drives"] = []
    result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
    for drive in result["drives"]:
        if drive["status"] == "available":
            mylog.debug("Adding driveID " + str(drive["driveID"]) + " (slot " + drive["slot"] + ") from nodeID " + str(drive["nodeID"]))
            newdrive = {}
            newdrive["driveID"] = drive["driveID"]
            newdrive["type"] = "automatic"
            params["drives"].append(newdrive)

    if len(params["drives"]) <= 0:
        mylog.passed("There are no available drives to add")

    mylog.info("Adding " + str(len(params["drives"])) + " drives to cluster")
    add_time = time.time();
    time.sleep(2)
    result = libsf.CallApiMethod(mvip, username, password, "AddDrives", params)

    if not no_sync:
        mylog.info("Waiting to make sure syncing has started")
        time.sleep(60)

        mylog.info("Waiting for slice syncing")
        while libsf.ClusterIsSliceSyncing(mvip, username, password):
            time.sleep(20)

        mylog.info("Waiting for bin syncing")
        # Make sure bin sync is done
        while libsf.ClusterIsBinSyncing(mvip, username, password):
            time.sleep(20)

    mylog.passed("Successfully added drives to the cluster")


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

