#!/usr/bin/python

# This script will print out the cssh command to connect to a cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass


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
    global mvip, username, password

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
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    # Get a list of nodes in the cluster
    node_list = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
    node_mips = []
    for node in node_list["nodes"]:
        node_mips.append(node["mip"])
    
    print " cssh -l root " + " ".join(node_mips)
    


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


