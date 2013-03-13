#!/usr/bin/python

# This script deletes all of the volumes for an account

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"    # The management VIP of the cluster
                            # --mvip

username = "admin"          # Admin account for the cluster
                            # --user

password = "password"      # Admin password for the cluster
                            # --pass

volume_name = ""            # The name of the volume to delete
                            # --volume_name

volume_id = 0               # The volumeID of the volume to delete
                            # --volume_id

volume_prefix = ""          # Prefix for the volumes to delete
                            # volume_name or volume_id will supercede this
                            # --volume_prefix

volume_regex = ""           # Regex to search for volumes to delete
                            # --volume_regex

volume_count = 0            # Delete at most this many volumes (0 to delete all matches)
                            # --volume_count

source_account = ""         # Account to use to search for volumes to delete
                            # Can be used with volume_prefix
                            # volume_name or volume_id will supercede this
                            # --source_account

purge = False               # Purge the volumes after deleting them
                            # --purge

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
    global mvip, username, password, source_account, volume_id, volume_name, volume_id, volume_prefix, volume_regex, volume_count

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
    parser.add_option("--volume_name", type="string", dest="volume_name", default=volume_name, help="the volume to delete")
    parser.add_option("--volume_id", type="int", dest="volume_id", default=volume_id, help="the volume to delete")
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=volume_prefix, help="the prefix of volumes to delete")
    parser.add_option("--volume_regex", type="string", dest="volume_regex", default=volume_regex, help="regex to search for volumes to delete")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=volume_count, help="the number of volumes to delete")
    parser.add_option("--source_account", type="string", dest="source_account", default=source_account, help="the name of the account to select volumes from")
    parser.add_option("--purge", action="store_true", dest="purge", help="purge the volumes after deleting them")
    parser.add_option("--test", action="store_true", dest="test", help="show the volumes that would be deleted but don't actually delete them")
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
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if options.purge:
        purge = True
    else:
        purge = False
    if options.test:
        test = True
    else:
        test = False
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    # Get a list of volumes to delete
    mylog.info("Searching for volumes")
    try:
        volumes_to_delete = libsf.SearchForVolumes(mvip, username, password, VolumeId=volume_id, VolumeName=volume_name, VolumeRegex=volume_regex, VolumePrefix=volume_prefix, AccountName=source_account, VolumeCount=volume_count)
    except SfError as e:
        mylog.error(e.message)
        sys.exit(1)

    count = len(volumes_to_delete.keys())
    names = ", ".join(sorted(volumes_to_delete.keys()))
    mylog.info("Deleting " + str(count) + " volumes: " + names)

    if test:
        mylog.info("Test option set; volumes will not be deleted")
        sys.exit(0)

    # Delete the requested volumes
    for vol_name in sorted(volumes_to_delete.keys()):
        vol_id = volumes_to_delete[vol_name]
        mylog.debug("Deleting " + vol_name)
        params = {}
        params["volumeID"] = vol_id
        volume_obj = libsf.CallApiMethod(mvip, username, password, "DeleteVolume", params)
        if purge:
            mylog.debug("Purging " + vol_name)
            volume_obj = libsf.CallApiMethod(mvip, username, password, "PurgeDeletedVolume", params)


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
