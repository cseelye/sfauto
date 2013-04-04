#!/usr/bin/python

# This script moves volumes from one account to another

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"    # The management VIP of the cluster
                            # --mvip

username = "admin"          # Admin account for the cluster
                            # --user

password = "password"      # Admin password for the cluster
                            # --pass

volume_name = ""            # The name of the volume to move
                            # --volume_name

volume_id = 0               # The volumeID of the volume to move
                            # --volume_id

volume_prefix = ""          # Prefix for the volumes to move
                            # volume_name or volume_id will supercede this
                            # --volume_prefix

volume_regex = ""           # Regex to search for volumes to move
                            # --volume_regex

volume_count = 0            # move at most this many volumes (0 to move all matches)
                            # --volume_count

source_account = ""         # Account to use to search for volumes to move
                            # Can be used with volume_prefix
                            # volume_name or volume_id will supercede this
                            # --source_account

source_account_id = 0       # Account to use to search for volumes to move
                            # Can be used with volume_prefix
                            # volume_name or volume_id will supercede this
                            # --source_account_id

dest_account = ""           # Account to move the volumes to
                            # --dest_account

dest_account_id = 0         # Account to move the volumes to
                            # --dest_account

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
    global mvip, username, password, source_account, source_account_id, dest_account, dest_account_id, volume_id, volume_name, volume_id, volume_prefix, volume_regex, volume_count

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
    parser.add_option("--volume_name", type="string", dest="volume_name", default=volume_name, help="the volume to move")
    parser.add_option("--volume_id", type="string", dest="volume_id", default=volume_id, help="the volume to move")
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=volume_prefix, help="the prefix of volumes to move")
    parser.add_option("--volume_regex", type="string", dest="volume_regex", default=volume_regex, help="regex to search for volumes to move")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=volume_count, help="the number of volumes to move")
    parser.add_option("--source_account", type="string", dest="source_account", default=source_account, help="the name of the account to select volumes from")
    parser.add_option("--source_account_id", type="int", dest="source_account_id", default=source_account_id, help="the ID of the account to select volumes from")
    parser.add_option("--dest_account", type="string", dest="dest_account", default=dest_account, help="the name of the account to move volumes to")
    parser.add_option("--dest_account_id", type="int", dest="dest_account_id", default=dest_account_id, help="the ID of the account to move volumes to")
    parser.add_option("--test", action="store_true", dest="test", help="show the volumes that would be moved but don't actually move them")
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
    dest_account = options.dest_account
    dest_account_id = options.dest_account_id
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

    # Find the destination account
    if dest_account:
        try:
            account_info = libsf.FindAccount(mvip, username, password, AccountName=dest_account)
        except SfError as e:
            mylog.error(str(e))
            sys.exit(1)
        dest_account_id = account_info["accountID"]

    # Get a list of volumes to move
    mylog.info("Searching for volumes")
    try:
        volumes_to_move = libsf.SearchForVolumes(mvip, username, password, VolumeId=volume_id, VolumeName=volume_name, VolumeRegex=volume_regex, VolumePrefix=volume_prefix, AccountName=source_account, AccountId=source_account_id, VolumeCount=volume_count)
    except SfError as e:
        mylog.error(e.message)
        sys.exit(1)

    count = len(volumes_to_move.keys())
    names = ", ".join(sorted(volumes_to_move.keys()))
    mylog.info(str(count) + " volumes wil be moved: " + names)

    if test:
        mylog.info("Test option set; volumes will not be moved")
        sys.exit(0)

    # move the requested volumes
    mylog.info("Moving volumes")
    for vol_name in sorted(volumes_to_move.keys()):
        vol_id = volumes_to_move[vol_name]
        mylog.debug("Moving " + vol_name)
        params = {}
        params["volumeID"] = vol_id
        params["accountID"] = dest_account_id
        volume_obj = libsf.CallApiMethod(mvip, username, password, "ModifyVolume", params, ApiVersion=5.0)

    mylog.passed("Successfully moved volumes")

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
