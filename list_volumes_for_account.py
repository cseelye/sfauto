#!/usr/bin/python

# This script will print the list of volumes for a specified account

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass

source_account = ""             # Account to list volumes from
                                # --source_account

# ----------------------------------------------------------------------------


import sys,os
from optparse import OptionParser
import json
import urllib2
import random
import platform
import time
import re
import multiprocessing
import libsf
from libsf import mylog
import libclient
from libclient import ClientError, SfClient


def main():
    global mvip, username, password, source_account

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster  [%default]")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster  [%default]")
    parser.add_option("--source_account", type="string", dest="source_account", default=source_account, help="the name of the account to list volumes from")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    source_account = options.source_account
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    # Get a list of accounts from the cluster
    accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})

    # Find the corresponding account on the cluster
    account_id = 0
    for account in accounts_list["accounts"]:
        if account["username"].lower() == source_account.lower():
            account_id = account["accountID"]
            break
    if account_id == 0:
        mylog.error("Could not find account " + source_account + " on " + mvip)
        sys.exit(1)

    volume_list = libsf.CallApiMethod(mvip, username, password, "ListVolumesForAccount", { "accountID" : account_id })
    for vol in volume_list["volumes"]:
        print vol["name"]


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


