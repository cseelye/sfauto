#!/usr/bin/python

# This script will get the capacity stats for a cluster.  The default behavior
# is to print all stats to the screen.  When used with the "stat" parameter, the
# behavior is to echo only that stat to the screen, such as for use in a bash
# script to be saved into a variable

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

stat = ""                           # Which capacity stat to get.  Default is all
                                    # --stat

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog


def main():
    global mvip, username, password, stat

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
    parser.add_option("--stat", type="string", dest="stat", default=stat, help="the capacity stat to get")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    stat = options.stat
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    result = libsf.CallApiMethod(mvip, username, password, "GetClusterCapacity", {})
    if stat != None and len(stat) > 0:
        sys.stdout.write(str(result["clusterCapacity"][stat]) + "\n")
    else:
        for key, value in result["clusterCapacity"].iteritems():
            mylog.info(str(key) + " = " + str(value))
    exit(0)


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

