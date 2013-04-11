#!/usr/bin/python

# This script will clear the SolidFire logs on one or more nodes

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

node_ips = [                        # The IP addresses of the nodes to monitor
    #"192.168.133.0",               # --node_ips
]

save_bundle = False                 # Save a support bundle before clearning the logs
                                    # --save_bundle

ssh_user = "root"                   # The SSH username for the nodes
                                    # --ssh_user

ssh_pass = "password"              # The SSH password for the nodes
                                    # --ssh_pass

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import paramiko
import re
import socket
import libsf
from libsf import mylog

def main():
    global node_ips, ssh_user, ssh_pass, save_bundle

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
    parser.add_option("--save_bundle", action="store_true", dest="save_bundle", default=save_bundle, help="save a support bundle before clearing logs")
    (options, args) = parser.parse_args()
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("Please enter a valid MVIP")
        sys.exit(1)
    try:
        node_ips = libsf.ParseIpsFromList(options.node_ips)
    except TypeError as e:
        mylog.error(e)
        sys.exit(1)
    if not node_ips:
        mylog.error("Please supply at least one node IP address")
        sys.exit(1)

    for node_ip in node_ips:
        mylog.info("Connecting to node '" + node_ip + "'")
        ssh = libsf.ConnectSsh(node_ip, ssh_user, ssh_pass)

        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "hostname")
        node_hostname = stdout.readlines()[0].strip()

        if save_bundle:
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "/sf/scripts/sf_make_support_bundle `date +%Y-%m-%d-%H-%M-%S`-`hostname`-supportbundle.tar")
            data = stdout.readlines()

        mylog.info("Clearing SF logs on '" + node_hostname + "'")
        # get rid of old logs
        libsf.ExecSshCommand(ssh, "rm -f /var/log/sf-*.gz")
        # empty current logs
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "ls -1 /var/log/sf-*")
        data = stdout.readlines()
        for filename in data:
            libsf.ExecSshCommand(ssh, "cat \"Log cleared on `date +%Y-%m-%d-%H-%M-%S`\" > " + filename)

        ssh.close()

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

