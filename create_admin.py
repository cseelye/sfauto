#!/usr/bin/python

# This script will create admin users on a cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass

admin_name = ""                 # The name for the new admin
                                # --admin_name

admin_pass = "password"        # The password for the new admin
                                # --admin_pass

admin_access = "administrator"        # Access level for the new admin
                                # --admin_access

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import libsf
from libsf import mylog

def main():
    global mvip, username, password, admin_name, admin_pass, admin_access

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
    parser.add_option("--admin_name", type="string", dest="admin_name", default=admin_name, help="the name for the new admin")
    parser.add_option("--admin_pass", type="string", dest="admin_pass", default=admin_pass, help="the password for the new admin")
    parser.add_option("--admin_access", type="string", dest="admin_access", default=admin_access, help="access level for the newadmin ")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    admin_name = options.admin_name
    admin_pass = options.admin_pass
    admin_access = options.admin_access
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    params = {}
    params["username"] = admin_name
    params["password"] = admin_pass
    params["access"] = [admin_access]
    result = libsf.CallApiMethod(mvip, username, password, "AddClusterAdmin", params)


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
