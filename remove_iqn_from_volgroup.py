#!/usr/bin/python

# This script will remove an initiator IQN from a VAG

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "solidfire"              # Admin password for the cluster
                                    # --pass

vag_name = ""                       # The name of the group to remove from
                                    # --vag_name

vag_id = 0                          # The ID of the group to remove from
                                    # --vag_id

iqn = ""                            # The initiator IQN to remove
                                    # --iqn

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog, SfError


def main():
    global mvip, username, password, vag_name, vag_id, iqn

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
    parser.add_option("--vag_name", type="string", dest="vag_name", default=vag_name, help="the name of the group")
    parser.add_option("--vag_id", type="int", dest="vag_id", default=vag_id, help="the ID of the group")
    parser.add_option("--iqn", type="string", dest="iqn", default=iqn, help="the initiator IQN to remove from the group")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    vag_name = options.vag_name
    vag_id = options.vag_id
    iqn = options.iqn
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    mylog.info("Removing " + iqn + " from group " + str(vag_name))

    # Find the group
    try:
        vag = libsf.FindVolumeAccessGroup(mvip, username, password, VagName=vag_name, VagId=vag_id)
    except SfError as e:
        mylog.error(str(e))
        sys.exit(1)

    found = False
    for existing_iqn in vag["initiators"]:
        if existing_iqn.lower() == iqn.lower():
            found = True
            break
    if not found:
        mylog.passed("IQN is already not in group")
        sys.exit(0)

    params = {}
    params["volumeAccessGroupID"] = vag["volumeAccessGroupID"]
    params["initiators"] = vag["initiators"]
    params["initiators"].remove(iqn)
    libsf.CallApiMethod(mvip, username, password, "ModifyVolumeAccessGroup", params, ApiVersion=5.0)

    mylog.passed("Removed IQN from group")


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
