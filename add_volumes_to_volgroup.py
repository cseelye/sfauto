#!/usr/bin/python

# This script adds volumes to a VAG

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"    # The management VIP of the cluster
                            # --mvip

username = "admin"          # Admin account for the cluster
                            # --user

password = "solidfire"      # Admin password for the cluster
                            # --pass

volume_name = ""            # The name(s) of the volume(s) to add
                            # --volume_name

volume_id = 0               # The volumeID(s) of the volume(s) to add
                            # --volume_id

volume_prefix = ""          # Prefix for the volumes to add
                            # volume_name or volume_id will supercede this
                            # --volume_prefix

volume_regex = ""           # Regex to search for volumes to add
                            # --volume_regex

volume_count = 0            # Add at most this many volumes (0 to add all matches)
                            # --volume_count

source_account = ""         # Account to use to search for volumes to add
                            # Can be used with volume_prefix
                            # volume_name or volume_id will supercede this
                            # --source_account

source_account_id = 0       # Account to use to search for volumes to add
                            # Can be used with volume_prefix
                            # volume_name or volume_id will supercede this
                            # --source_account_id

vag_name = ""               # Group to add the volumes to
                            # --vag_name

vag_id = 0                  # Group to add the volumes to
                            # --vag_id

# ----------------------------------------------------------------------------


import sys,os
from optparse import OptionParser
import json
import urllib2
import random
import platform
import time
import libsf
from libsf import mylog, SfError

def main():
    global mvip, username, password, source_account, source_account_id, vag_name, vag_id, volume_id, volume_name, volume_id, volume_prefix, volume_regex, volume_count

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
    parser.add_option("--volume_name", type="string", dest="volume_name", default=volume_name, help="the volume to add")
    parser.add_option("--volume_id", type="string", dest="volume_id", default=volume_id, help="the volume to add")
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=volume_prefix, help="the prefix of volumes to add")
    parser.add_option("--volume_regex", type="string", dest="volume_regex", default=volume_regex, help="regex to search for volumes to add")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=volume_count, help="the number of volumes to add")
    parser.add_option("--source_account", type="string", dest="source_account", default=source_account, help="the name of the account to select volumes from")
    parser.add_option("--source_account_id", type="int", dest="source_account_id", default=source_account_id, help="the ID of the account to select volumes from")
    parser.add_option("--vag_name", type="string", dest="vag_name", default=vag_name, help="the name of the VAG to add volumes to")
    parser.add_option("--vag_id", type="int", dest="vag_id", default=vag_id, help="the ID of the VAG to add volumes to")
    parser.add_option("--test", action="store_true", dest="test", help="show the volumes that would be added but don't actually add them")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    volume_name = options.volume_name
    volume_id = options.volume_id
    volume_prefix = options.volume_prefix
    volume_regex = options.volume_regex
    volume_count = options.volume_count
    source_account = options.source_account
    source_account_id = options.source_account_id
    vag_name = options.vag_name
    vag_id = options.vag_id
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if options.test:
        test = True
    else:
        test = False
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    # Find the destination VAG
    try:
        vag = libsf.FindVolumeAccessGroup(mvip, username, password, VagName=vag_name, VagId=vag_id)
    except SfError as e:
        mylog.error(str(e))
        sys.exit(1)

    # Get a list of volumes to add
    mylog.info("Searching for volumes")
    try:
        volumes_to_add = libsf.SearchForVolumes(mvip, username, password, VolumeId=volume_id, VolumeName=volume_name, VolumeRegex=volume_regex, VolumePrefix=volume_prefix, AccountName=source_account, AccountId=source_account_id, VolumeCount=volume_count)
    except SfError as e:
        mylog.error(e.message)
        sys.exit(1)

    count = len(volumes_to_add.keys())
    names = ", ".join(sorted(volumes_to_add.keys()))
    mylog.info(str(count) + " volumes wil be added: " + names)

    if test:
        mylog.info("Test option set; volumes will not be added")
        sys.exit(0)

    volume_ids = vag["volumes"]
    for vol_name, vol_id in volumes_to_add.iteritems():
        if vol_id in vag["volumes"]:
            mylog.debug(vol_name + " is already in group")
        else:
            volume_ids.append(vol_id)

    # Add the requested volumes
    mylog.info("Adding volumes to group")
    params = {}
    params["volumes"] = volume_ids
    params["volumeAccessGroupID"] = vag["volumeAccessGroupID"]
    volume_obj = libsf.CallApiMethod(mvip, username, password, "ModifyVolumeAccessGroup", params, ApiVersion=5.0)

    mylog.passed("Successfully added volumes to group")


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
