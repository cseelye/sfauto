#!/usr/bin/python

"""
This script will display volume states as they change

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var
"""

import sys
from optparse import OptionParser
import logging
import json
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults

def ValidateArgs(args):
    libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                        "username" : None,
                        "password" : None},
        args)

def Execute(mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
    """
    Continuously show changing volume states
    """
    ValidateArgs(locals())
    if debug:
        mylog.console.setLevel(logging.DEBUG)

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

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.debug):
            sys.exit(0)
        else:
            sys.exit(1)
    except libsf.SfArgumentError as e:
        mylog.error("Invalid arguments - \n" + str(e))
        sys.exit(1)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

