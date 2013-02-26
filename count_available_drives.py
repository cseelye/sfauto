#!/usr/bin/env python

# This script will count drives in the available pool and fail if there is more than the expected number

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.0.0"        # The management VIP for the cluster
                            # --mvip

username = "admin"          # The API username for the nodes
                            # --api_user

password = "password"      # The API password for the nodes
                            # --api_pass

expected = 0                # The expected number of drives
                            # --expected

compare = "le"              # Comparison method to use
                            # lt, le, gt, ge, eq
                            # --compare

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import libsf
from libsf import mylog


def main():
    global mvip, username, password, expected, compare

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line options
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management VIP for the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the username for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the password for the cluster")
    parser.add_option("--expected", type="string", dest="expected", default=expected, help="the expected number of drives")
    parser.add_option("--compare", type="string", dest="compare", default=compare, help="the comparison operator to use")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    username = options.username
    password = options.password
    mvip = options.mvip
    expected = options.expected
    compare = options.compare
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    if compare == "eq": op = "=="
    elif compare == "lt": op = "<"
    elif compare == "le": op = "<="
    elif compare == "gt": op = ">"
    elif compare == "ge": op = ">="
    else:
        mylog.error("Unknown operator '" + compare + "'")
        sys.exit(1)

    mylog.info("Searching for available drives...")
    available_count = 0
    result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
    for drive in result["drives"]:
        if drive["status"] == "available":
            available_count += 1

    expression = str(available_count) + " " + op + " " + str(expected)
    mylog.debug("Testing " + expression)
    result = eval(expression)

    if result:
        mylog.passed("Found " + str(available_count) + " drives")
        sys.exit(0)
    else:
        mylog.error("Found " + str(available_count) + " drives but expected " + op + " " + str(expected))
        sys.exit(1)


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

