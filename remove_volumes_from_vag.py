#!/usr/bin/python

# This script removes volumes from a VAG

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"    # The management VIP of the cluster
                            # --mvip

username = "admin"          # Admin account for the cluster
                            # --user

password = "solidfire"      # Admin password for the cluster
                            # --pass

volume_name = ""            # The name(s) of the volume(s) to remove
                            # --volume_name

volume_id = 0               # The volumeID(s) of the volume(s) to remove
                            # --volume_id

volume_prefix = ""          # Prefix for the volumes to remove
                            # volume_name or volume_id will supercede this
                            # --volume_prefix

volume_regex = ""           # Regex to search for volumes to remove
                            # --volume_regex

volume_count = 0            # Remove at most this many volumes (0 to remove all matches)
                            # --volume_count

source_account = ""         # Account to use to search for volumes to remove
                            # Can be used with volume_prefix
                            # volume_name or volume_id will supercede this
                            # --source_account

source_account_id = 0       # Account to use to search for volumes to remove
                            # Can be used with volume_prefix
                            # volume_name or volume_id will supercede this
                            # --source_account_id

vag_name = ""               # Group to remove the volumes from
                            # --vag_name

vag_id = 0                  # Group to remove the volumes from
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
    parser.add_option("--volume_name", type="string", dest="volume_name", default=volume_name, help="the volume to remove")
    parser.add_option("--volume_id", type="string", dest="volume_id", default=volume_id, help="the volume to remove")
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=volume_prefix, help="the prefix of volumes to remove")
    parser.add_option("--volume_regex", type="string", dest="volume_regex", default=volume_regex, help="regex to search for volumes to remove")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=volume_count, help="the number of volumes to remove")
    parser.add_option("--source_account", type="string", dest="source_account", default=source_account, help="the name of the account to select volumes from")
    parser.add_option("--source_account_id", type="int", dest="source_account_id", default=source_account_id, help="the ID of the account to select volumes from")
    parser.add_option("--vag_name", type="string", dest="vag_name", default=vag_name, help="the name of the VAG to remove volumesfrom")
    parser.add_option("--vag_id", type="int", dest="vag_id", default=vag_id, help="the ID of the VAG to remove volumes from")
    parser.add_option("--test", action="store_true", dest="test", help="show the volumes that would be removed but don't actually remove them")
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

    if volume_id:
        # Shortcut if we have a list of IDs - assume they are valid and the user knows what they are doing
        volume_id_list = []
        if isinstance(volume_id, basestring):
            volume_id_list = volume_id.split(",")
            volume_id_list = map(int, volume_id_list)
        else:
            try:
                volume_id_list = list(volume_id)
            except ValueError:
                volume_id_list.append(volume_id)

        vag_volumes = vag["volumes"]
        for volume_id in volume_id_list:
            if volume_id not in vag_volumes:
                mylog.debug(str(volume_id) + " is already not in group")
            else:
                vag_volumes.remove(volume_id)

    else:
        # Get a list of volumes to remove
        mylog.info("Searching for volumes")
        try:
            volumes_to_remove = libsf.SearchForVolumes(mvip, username, password, VolumeId=volume_id, VolumeName=volume_name, VolumeRegex=volume_regex, VolumePrefix=volume_prefix, AccountName=source_account, AccountId=source_account_id, VolumeCount=volume_count)
        except SfError as e:
            mylog.error(e.message)
            sys.exit(1)

        count = len(volumes_to_remove.keys())
        names = ", ".join(sorted(volumes_to_remove.keys()))
        mylog.info(str(count) + " volumes wil be removed: " + names)

        vag_volumes = vag["volumes"]
        for vol_name, vol_id in volumes_to_remove.iteritems():
            if vol_id not in vag_volumes:
                mylog.debug(vol_name + " is already not in group")
            else:
                vag_volumes.remove(vol_id)

    if test:
        mylog.info("Test option set; volumes will not be removed")
        sys.exit(0)

    # Remove the requested volumes
    mylog.info("Removing volumes from group")
    params = {}
    params["volumes"] = vag_volumes
    params["volumeAccessGroupID"] = vag["volumeAccessGroupID"]
    volume_obj = libsf.CallApiMethod(mvip, username, password, "ModifyVolumeAccessGroup", params, ApiVersion=5.0)

    mylog.passed("Successfully removed volumes from group")


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
