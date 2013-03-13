#!/usr/bin/python

# This script will force whole file sync form the primary to the secondary

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "solidfire"              # Admin password for the cluster
                                    # --pass

volume_names = [                    # The names of the volumes to sync
                                    # --volume_names
]

volume_ids = [                       # The volumeIDs of the volumes to sync
                                    # --volume_ids
]

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import json
import libsf
from libsf import mylog


def main():
    global mvip, username, password, volume_ids, volume_names

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
    parser.add_option("--volume_names", type="string", dest="volume_names", default=",".join(volume_names), help="the volume to sync")
    parser.add_option("--volume_ids", type="string", dest="volume_ids", default=",".join(volume_ids), help="the volume to sync")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    volume_names = options.volume_names
    volume_ids = options.volume_ids
    if volume_names:
        volume_names = volume_names.split(",")
    if volume_ids:
        pieces = volume_ids.split(",")
        try:
            volume_ids = map(int, pieces)
        except ValueError:
            mylog.error("Invalid volume ID")
            sys.exit(1)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    if volume_names:
        mylog.info("Searching for volumes " + str(volume_names))
        try:
            found_volumes = libsf.SearchForVolumes(mvip, username, password, VolumeName=volume_names)
        except SfError as e:
            mylog.error(e.message)
            sys.exit(1)
        volume_ids = found_volumes.values()
    
    if len(volume_ids) <= 0:
        mylog.error("Please enter volume_ids or volume_names")
        sys.exit(1)

    # Find the primary and secondary SS for each volume, and force a sync from primary to secondary
    mylog.info("Finding primary/secondary SS")
    slice_report = libsf.HttpRequest("https://" + str(mvip) + "/reports/slices.json", username, password)
    slice_json = json.loads(slice_report)
    for volume_id in volume_ids:
        for slice_obj in slice_json["slices"]:
            if slice_obj["volumeID"] == volume_id:
                primary = slice_obj["primary"]
                secondary = slice_obj["liveSecondaries"][0]
                mylog.info("Forcing whole sync of volume " + str(volume_id) + " from slice" + str(primary) + " to slice" + str(secondary))
                params = {}
                params["sliceID"] = volume_id
                params["primary"] = primary
                params["secondary"] = secondary
                result = libsf.CallApiMethod(mvip, username, password, "ForceWholeFileSync", params, ApiVersion=5.0)
                break


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

