#!/usr/bin/python

# This script will deleye a CHAP account on the clsuter

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"    # The management VIP of the cluster
                            # --mvip

username = "admin"          # Admin account for the cluster
                            # --user

password = "password"      # Admin password for the cluster
                            # --pass

account_name = ""           # The name of the account to delete
                            # --account_name

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog


def main():
    global mvip, username, password, account_name, initiator_secret, target_secret

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
    parser.add_option("--account_name", type="string", dest="account_name", default=account_name, help="the name for the account")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    account_name = options.account_name
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    # Find the account to delete
    mylog.info("Searching for account " + account_name)
    accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
    account_id = 0
    for account in accounts_list["accounts"]:
        if account["username"].lower() == account_name.lower():
            account_id = account["accountID"]
            break
    if account_id == 0:
        mylog.error("Could not find account " + account_name + " on cluster " + mvip)
        sys.exit(1)
    
    mylog.info("Deleting account ID " + str(account_id))
    result = libsf.CallApiMethod(mvip, username, password, "RemoveAccount", {"accountID" : account_id})

    mylog.passed("Successfully deleted account " + account_name)

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







