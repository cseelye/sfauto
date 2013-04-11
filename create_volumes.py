#!/usr/bin/python

# This script will create volumes on an sf cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass

volume_prefix = "a"             # Prefix for the volume. Name will be generated as volume_prefix + "%05d"
                                # --volume_prefix

volume_count = 20               # The number of volumes to create
                                # --volume_count

volume_size = 0                 # The volume size in GB
                                # --volume_size

volume_start = 1                # The volume number to start from
                                # --volume_start

enable_512 = False              # Use 512e on the volumes
                                # --512e

max_iops = 100000               # QoS max IOPs
                                # --max_iops

min_iops = 100                  # QoS min IOPs
                                # --min_iops

burst_iops = 100000             # QoS burst IOPs
                                # --burst_iops

account_name = "myhostname"     # The name of the account to create the volumes for
                                # If account_id > 0 is specified, it will be used instead of account_name
                                # Either account_name or account_id must be specified
                                # --account_name

account_id = 0                  # The account ID to create the volumes for
                                # Values <= 0 will be ignored and account_name will be used instead
                                # Either account_name or account_id must be specified
                                # --account_id

wait = 0                        # How long to wait between creating each volume (seconds)
                                # --wait
# ----------------------------------------------------------------------------


import sys,os
from optparse import OptionParser
import json
import urllib2
import random
import platform
import time
import libsf
from libsf import mylog

def main():
    global mvip, username, password, volume_prefix, volume_count, volume_size, volume_start, max_iops, min_iops, burst_iops, account_name, account_id, wait

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
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=volume_prefix, help="the prefix for the volume (volume name will be volume prefix + %05d)")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=volume_count, help="the number of volumes to create")
    parser.add_option("--volume_size", type="int", dest="volume_size", default=volume_size, help="the volume size in GB")
    parser.add_option("--volume_start", type="int", dest="volume_start", default=volume_start, help="the volume number to start from")
    parser.add_option("--max_iops", type="int", dest="max_iops", default=max_iops, help="the max sustained IOPS to allow on this volume")
    parser.add_option("--min_iops", type="int", dest="min_iops", default=min_iops, help="the min sustained IOPS to guarentee on this volume")
    parser.add_option("--burst_iops", type="int", dest="burst_iops", default=burst_iops, help="the burst IOPS to allow on this volume")
    parser.add_option("--512e", action="store_true", dest="enable_512", help="use 512 sector emulation")
    parser.add_option("--account_name", type="string", dest="account_name", default=account_name, help="the account to create the volumes for (either name or id must be specified)")
    parser.add_option("--account_id", type="int", dest="account_id", default=account_id, help="the account to create the volumes for (either name or id must be specified)")
    parser.add_option("--wait", type="int", dest="wait", default=wait, help="how long to wait between creating each volume (seconds)")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")

    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    volume_prefix = options.volume_prefix
    volume_count = options.volume_count
    volume_size = options.volume_size
    volume_start = options.volume_start
    max_iops = options.max_iops
    min_iops = options.min_iops
    burst_iops = options.burst_iops
    enable_512 = options.enable_512
    account_name = options.account_name.lower()
    account_id = options.account_id
    wait = options.wait
    if (enable_512 == None):
        enable_512 = False
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    mylog.info("Management VIP = " + mvip)
    mylog.info("Username       = " + username)
    mylog.info("Password       = " + password)
    mylog.info("Volume prefix  = " + volume_prefix)
    mylog.info("Volume size    = " + str(volume_size) + " GB")
    mylog.info("Volume count   = " + str(volume_count))
    mylog.info("Max IOPS       = " + str(max_iops))
    mylog.info("Min IOPS       = " + str(min_iops))
    mylog.info("Burst IOPS     = " + str(burst_iops))
    mylog.info("512e           = " + str(enable_512))


    # Search for account name/id
    if (account_id > 0):
        account_name = None
    accounts_obj = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
    for account in accounts_obj["accounts"]:
        if (account_id <= 0):
            if (account["username"].lower() == account_name.lower()):
                account_id = account["accountID"]
                break
        else:
            if (account["accountID"] == account_id):
                account_name = account["username"]
                break

    if (account_id <= 0):
        mylog.error("Could not find account with name '" + account_name + "'")
        sys.exit(1)
    if (account_name == None):
        mylog.error("Could not find account '" + account_id + "'")
        sys.exit(1)

    mylog.info("Account name   = " + account_name)
    mylog.info("Account ID     = " + str(account_id))


    # Create the requested volumes
    created = 0
    for vol_num in range(volume_start, volume_start + volume_count):
        volume_name = volume_prefix + "%05d" % vol_num
        params = {}
        params["name"] = volume_name
        params["accountID"] = account_id
        params["totalSize"] = int(volume_size * 1000 * 1000 * 1000)
        params["enable512e"] = enable_512
        qos = {}
        qos["maxIOPS"] = max_iops
        qos["minIOPS"] = min_iops
        qos["burstIOPS"] = burst_iops
        params["qos"] = qos
        volume_obj = libsf.CallApiMethod(mvip, username, password, "CreateVolume", params)
        mylog.info("Created volume " + volume_name)
        created += 1
        if (wait > 0):
            time.sleep(wait)

    if (created == volume_count):
        mylog.passed("Successfully created " + str(volume_count) + " volumes")
        exit(0)
    else:
        mylog.red("Could not create all volumes")
        exit(1)


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


