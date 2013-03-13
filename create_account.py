#!/usr/bin/python

# This script will create a CHAP account on the clsuter

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

account_name = ""                   # The name of the account to create
                                    # --account_name

initiator_secret = ""               # The initiator secret to use. Leave blank to auto-create
                                    # --initiator_secret

target_secret = ""                  # The target secret to use. Leave blank to auto-create
                                    # --target_secret

strict = False                      # Fail if the account already exists
                                    # --strict

# ----------------------------------------------------------------------------

import sys,os,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog


def main():
    global mvip, username, password, account_name, initiator_secret, target_secret, strict

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
    parser.add_option("--initiator_secret", type="string", dest="initiator_secret", default=initiator_secret, help="the initiator secret for the account")
    parser.add_option("--target_secret", type="string", dest="target_secret", default=target_secret, help="the target secret for the account")
    parser.add_option("--strict", action="store_true", dest="strict", help="fail if the account already exists")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    account_name = options.account_name
    initiator_secret = options.initiator_secret
    target_secret = options.target_secret
    if options.strict: strict = True
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    mylog.info("Creating account '" + str(account_name) + "'")
    params = {}
    params["username"] = account_name
    if initiator_secret != None and len(initiator_secret) > 0:
        params["initiatorSecret"] = initiator_secret
    else:
        params["initiatorSecret"] = libsf.MakeSimpleChapSecret()
    if target_secret != None and len(target_secret) > 0:
        params["targetSecret"] = target_secret
    else:
        params["targetSecret"] = libsf.MakeSimpleChapSecret()
    try:
        result = libsf.CallApiMethod(mvip, username, password, "AddAccount", params, ExitOnError=False)
    except libsf.SfApiError as e:
        if (e.name == "xDuplicateUsername" and not strict):
            mylog.passed("Account already exists")
            sys.exit(0)
        else:
            mylog.error(str(e))
            sys.exit(1)
    
    mylog.passed("Account created successfully")


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







