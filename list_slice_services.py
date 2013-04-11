#!/usr/bin/python

# This script will display the list of slice services in the cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass

csv = False                     # Display minimal output that is suitable for piping into other programs
                                # --csv

bash = False                    # Display minimal output that is formatted for a bash array/for  loop
                                # --bash

# ----------------------------------------------------------------------------

import sys, os
import os
from optparse import OptionParser
import time
from random import randint
import libsf
from libsf import mylog


def main():
    global mvip, username, password, csv, bash

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
    parser.add_option("--csv", action="store_true", dest="csv", help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", help="display a minimal output that is formatted for a bash array/for loop")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    if options.csv:
        csv = True
        mylog.silence = True
    if options.bash:
        bash = True
        mylog.silence = True
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)


    # Find the MIP of the cluster master
    result = libsf.CallApiMethod(mvip, username, password, 'ListServices', {})

    if csv or bash:
        separator = ","
        if bash: separator = " "
        
        slices = []
        for item in result["services"]:
            if 'service' in item and item['service']['serviceType'] == "slice":
                slices.append(str(item['service']['serviceID']))
        sys.stdout.write(separator.join(slices) + "\n")
        sys.stdout.flush()
    else:
        for item in result["services"]:
            if 'service' in item and item['service']['serviceType'] == "slice":
                mylog.info("slice" + str(item['service']['serviceID']))


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

