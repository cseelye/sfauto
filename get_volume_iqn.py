#!/usr/bin/python

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

volume_name = ""                    # The name of the volume to get the IQN from
                                    # --volume_name

csv = False                     # Display minimal output that is suitable for piping into other programs
                                # --csv

bash = False                    # Display minimal output that is formatted for a bash array/for  loop
                                # --bash

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog

def main():
    global mvip, username, password, volume_name, csv, bash

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--volume_name", type="string", dest="volume_name", default=volume_name, help="the volume to to get the IQN from")
    parser.add_option("--csv", action="store_true", dest="csv", help="display a minimal output that is suitable for piping into other programs" )
    parser.add_option("--bash", action="store_true", dest="bash", help="display a minimal output that is formatted for a bash array/for loop")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    volume_name = options.volume_name
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if options.csv:
        csv = True
        mylog.silence = True
    if options.bash:
        bash = True
        mylog.silence = True
    else:
        csv = False
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    all_volumes = libsf.CallApiMethod(mvip, username, password, "ListActiveVolumes", {})
    volume_iqn = None
    for volume in all_volumes["volumes"]:
        if volume["name"] == volume_name:
            volume_iqn = volume["iqn"]
            break
    if volume_iqn == None:
        mylog.error("Could not find volume '" + volume_name + "'")
        exit(1)
    
    if csv or bash:
        print volume_iqn
    else:
        mylog.info(volume_iqn)


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
