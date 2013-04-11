#!/usr/bin/python

# This script will set the clustername in solidfire.json on a list of nodes

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

node_ip = "192.168.000.000"     # The IP addresses of the nodes
                                # --node_ips

ssh_user = "root"               # The SSH username for the nodes
                                # --node_user

ssh_pass = "password"         # The SSH password for the nodes

new_time = "now"                # The new time to set - string passed directly to the "date" command
                                # --new_time

# ----------------------------------------------------------------------------


import sys, os
from optparse import OptionParser
import tempfile
import json
import re
import os
import time
import libsf
from libsf import mylog
try:
    import ssh
except ImportError:
    import paramiko as ssh


def main():
    # Parse command line arguments
    parser = OptionParser()
    global node_ip, ssh_user, ssh_pass, new_time
    parser.add_option("--node_ip", type="string", dest="node_ip", default=node_ip, help="the IP address of the node")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--new_time", type="string", dest="new_time", default=new_time, help="the new time to set on the node(s)")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    node_ip = options.node_ip
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    new_time = options.new_time
    if not libsf.IsValidIpv4Address(node_ip):
        mylog.error("Please supply a valid node IP")
        sys.exit(1)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)


    # Connect to the node
    mylog.info("Connecting to " + node_ip)
    ssh = libsf.ConnectSsh(node_ip, ssh_user, ssh_pass)

    stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "date -s \"" + new_time + "\"; echo $?")
    lines = stdout.readlines();
    if (int(lines.pop().strip()) != 0):
        mylog.error(node_ip + ": Failed to set time - " + "\n".join(stderr.readlines()))
        sys.exit(1)
    node_time = lines[0].strip()
    mylog.info("Set time to " + node_time)
    mylog.passed("Successfully set time on " + node_ip)



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

