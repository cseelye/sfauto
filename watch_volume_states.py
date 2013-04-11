#!/usr/bin/python

# This script will watch for degraded volumes on the cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "solidfire"              # Admin password for the cluster
                                    # --pass

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog
import json

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
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    volmap = dict()
    result = libsf.CallApiMethod(mvip, username, password, "ListActiveVolumes", {})
    for vol in result["volumes"]:
        volmap[vol["volumeID"]] = vol["name"]
    result = libsf.CallApiMethod(mvip, username, password, "ListDeletedVolumes", {})
    for vol in result["volumes"]:
        volmap[vol["volumeID"]] = vol["name"]

    previous_state = dict()

    while True:
        try:
            result = libsf.HttpRequest("https://" + mvip + "/reports/slices.json", username, password)
            slice_report = json.loads(result)
        except TypeError:
            continue

        # Make sure there are no unhealthy services
        #for ss in slice_report["services"]:
            #if ss["health"].lower() == "degraded":
            #    mylog.warning("slice" + str(ss["serviceID"]) + " status is " + ss["health"])
            #elif ss["health"].lower() != "good":
            #    mylog.error("slice" + str(ss["serviceID"]) + " status is " + ss["health"])

        # Make sure there are no volumes with multiple live secondaries or dead secondaries
        for vol in slice_report["slices"]:
            if "liveSecondaries" not in vol:
                if vol["volumeID"] in previous_state and previous_state[vol["volumeID"]] == "nolivesec":
                    continue

                if vol["volumeID"] in volmap:
                    mylog.error("%17s" % volmap[vol["volumeID"]] + " (volumeID " + str(vol["volumeID"]) + ") has no live secondaries")
                else:
                    mylog.error("%17s" % "" + "  volumeID " + str(vol["volumeID"]) + "  has no live secondaries")

                previous_state[vol["volumeID"]] = "nolivesec"

            elif len(vol["liveSecondaries"]) > 1:
                if vol["volumeID"] in previous_state and previous_state[vol["volumeID"]] == "multiplelivesec":
                    continue

                if vol["volumeID"] in volmap:
                    mylog.info("%17s" % volmap[vol["volumeID"]] + " (volumeID " + str(vol["volumeID"]) + ") has multiple live secondaries")
                else:
                    mylog.info("%17s" % "" + "  volumeID " + str(vol["volumeID"]) + "  has multiple live secondaries")

                previous_state[vol["volumeID"]] = "multiplelivesec"

            #if "deadSecondaries" in vol and len(vol["deadSecondaries"]) > 0:
            #    mylog.error("volumeID " + str(vol["volumeID"]) + " has dead secondaries")

            else:
                if vol["volumeID"] not in previous_state:
                    continue
                if vol["volumeID"] in previous_state and previous_state[vol["volumeID"]] == "good":
                    continue

                if vol["volumeID"] in volmap:
                    mylog.passed("%17s" % volmap[vol["volumeID"]] + " (volumeID " + str(vol["volumeID"]) + ") is healthy again")
                else:
                    mylog.passed("%17s" % "" + "  volumeID " + str(vol["volumeID"]) + "  is healthy again")

                previous_state[vol["volumeID"]] = "good"


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        #timer = libsf.ScriptTimer()
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

