#!/usr/bin/python

# This script will create a volume access group on the cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "solidfire"              # Admin password for the cluster
                                    # --pass

vag_name = ""                       # The name of the group to create
                                    # --vag_name

strict = False                      # Fail if the group already exists
                                    # --strict

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog, SfError

def main():
    global mvip, username, password, vag_name, strict

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
    parser.add_option("--vag_name", type="string", dest="vag_name", default=vag_name, help="the name for the group")
    parser.add_option("--strict", action="store_true", dest="strict", help="fail if the account already exists")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    vag_name = options.vag_name
    if options.strict: strict = True
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    mylog.info("Creating VAG '" + str(vag_name) + "'")

    # See if the group already exists
    try:
        libsf.FindVolumeAccessGroup(mvip, username, password, VagName=vag_name)
        if strict:
            mylog.error("Group already exists")
            sys.exit(1)
        else:
            mylog.passed("Group already exists")
            sys.exit(0)
    except SfError:
        # Group does not exist
        pass

    # Create the group
    params = {}
    params["name"] = vag_name
    params["initiators"] = []
    result = libsf.CallApiMethod(mvip, username, password, "CreateVolumeAccessGroup", params, ApiVersion=5.0)

    mylog.passed("Group created successfully")


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
