#!/usr/bin/python

# This script will show the version of solidfire on a list of nodes

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

node_ips = [                      # The IP addresses of the nodes
    "192.168.000.000",              # --node_ips
]

ssh_user = "root"                   # The SSH username for the nodes
                                    # --ssh_user

ssh_pass = "password"              # The SSH password for the nodes
                                    # --ssh_pass

# ----------------------------------------------------------------------------


from optparse import OptionParser
import paramiko
import re
import socket
import sys,os
import libsf
from libsf import mylog


def main():
    global node_ips, node_user, node_pass

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "node_ips" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]
    if isinstance(node_ips, basestring):
        node_ips = node_ips.split(",")


    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--node_ips", type="string", dest="node_ips", default=",".join(node_ips), help="the IP addresses of the nodes")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=ssh_user, help="the SSH username for the nodes")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=ssh_pass, help="the SSH password for the nodes")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    try:
        node_ips = libsf.ParseIpsFromList(options.node_ips)
    except TypeError as e:
        mylog.error(e)
        sys.exit(1)
    if not node_ips:
        mylog.error("Please supply at least one node IP address")
        sys.exit(1)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)

    for node_ip in node_ips:
        hostname = libsf.GetHostname(node_ip, ssh_user, ssh_pass)
        version = libsf.GetSfVersion(node_ip)
        mylog.info(node_ip + " [" + hostname + "]: " + version)


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

