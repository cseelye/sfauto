#!/usr/bin/python

# This script will wait for there to be no slice syncing on the cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "solidfire"              # Admin password for the cluster
                                    # --pass

wait_threshold = 0                  # How long to wait before considering this sync as taking "too long", in min
                                    # Set to 0 to disable
                                    # --wait_threshold

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog


def main():
    global mvip, username, password, since_timestamp, wait_threshold, email_notify

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password", "email_notify" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--wait_threshold", type="int", dest="wait_threshold", default=wait_threshold, help="give a warning if sync takes longer than this many minutes")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    wait_threshold = options.wait_threshold
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    mylog.info("Waiting for there to be no slice syncing on " + mvip)
    start_time = time.time()
    while libsf.ClusterIsSliceSyncing(mvip, username, password):
        time.sleep(30)
    end_time = time.time()
    duration = end_time - start_time

    if wait_threshold > 0 and duration / 60 > wait_threshold:
        mylog.error("Duration " + libsf.SecondsToElapsedStr(duration))
        mylog.error("Sync took too long")
        sys.exit(1)
    else:
        mylog.info("Duration " + libsf.SecondsToElapsedStr(duration))
        mylog.passed("Slice syncing is finished")

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
