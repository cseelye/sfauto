#!/usr/bin/python

# This script will wait GC to start and finish
# The script will complete after finding the first complete GC after the "since" time
# "since" can be in the past or future; default is the current time

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

since_timestamp = 0                 # When to start looking for GC (unix timestamp)
                                    # --since

wait_threshold = 0                  # How long to wait before considering this GC as taking "too long", in min
                                    # Set to 0 to disable
                                    # --wait_threshold

email_notify = ""                   # email address to send "too long" warning to
                                    # leave empty for no notification
                                    # --email_notify

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
    parser.add_option("--since", type="int", dest="since", default=since_timestamp, help="when to start looking for GC (unix timestamp)")
    parser.add_option("--wait_threshold", type="int", dest="wait_threshold", default=wait_threshold, help="give a warning if GC takes longer than this many minutes")
    parser.add_option("--email_notify", type="string", dest="email_notify", default=email_notify, help="email address to send the warning to")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    since_timestamp = options.since
    wait_threshold = options.wait_threshold
    email_notify = options.email_notify
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    if (since_timestamp <= 0):
        since_timestamp = time.time()

    result = libsf.CallApiMethod(mvip, username, password, "GetClusterCapacity", {})
    mylog.info("Cluster at " + mvip + " is " + libsf.HumanizeDecimal(result["clusterCapacity"]["provisionedSpace"], 1, "G") + " provisioned and " + libsf.HumanizeDecimal(result["clusterCapacity"]["usedSpace"], 1, "G") + "B used")

    (gc_start_time, gc_end_time, bytes_discarded) = libsf.WaitForGC(mvip, username, password, since_timestamp, wait_threshold, email_notify)
    if wait_threshold > 0 and gc_end_time - gc_start_time > wait_threshold*60:
        exit(1)
    else:
        exit(0)


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







