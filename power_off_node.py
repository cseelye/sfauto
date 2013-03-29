#!/usr/bin/python

# This script will power off a cluster node

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

node_ip = "192.168.000.000"     # The management IP of the node to power off
                                # --node_ip

ssh_user = "root"               # The username for the nodes
                                # --ssh_user

ssh_pass = "password"         # The password for the nodes
                                # --ssh_pass

ipmi_ip = None                  # The IPMI IP address of the node to power off
                                # If None, the script will determine the IP itself
                                # --ipmi_ip

ipmi_user = "root"              # The username for IPMI/DRAC
                                # --ipmi_user

ipmi_pass = "calvin"            # The password for IPMI/DRAC
                                # --ipmi_pass

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog

def main():
    global node_ip, ssh_user, ssh_pass, ipmi_ip, ipmi_user, ipmi_pass
    env_enabled_vars = [ "ipmi_user", "ipmi_pass" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--node_ip", type="string", dest="node_ip", default=node_ip, help="the management IP of the node to power off")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up. [%default]")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up, [%default]")
    parser.add_option("--ipmi_ip", type="string", dest="ipmi_ip", default=ipmi_ip, help="the IPMI IP of the node to power off. If not specified, the script will determine the IP")
    parser.add_option("--ipmi_user", type="string", dest="ipmi_user", default=ipmi_user, help="the IPMI username for the nodes [%default]")
    parser.add_option("--ipmi_pass", type="string", dest="ipmi_pass", default=ipmi_pass, help="the IPMI password for the nodes [%default]")
    parser.add_option("--nowait", action="store_true", dest="nowait", help="do not wait for the node to come back up")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    node_ip = options.node_ip
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    ipmi_ip = options.ipmi_ip
    ipmi_user = options.ipmi_user
    ipmi_pass = options.ipmi_pass
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(node_ip):
        mylog.error("'" + node_ip + "' does not appear to be a valid IP")
        sys.exit(1)

    if not ipmi_ip:
        mylog.info("Determining IPMI IP address for " + node_ip)
        ipmi_ip = libsf.GetIpmiIp(node_ip, ssh_user, ssh_pass)

    if not libsf.IsValidIpv4Address(ipmi_ip):
        mylog.error("'" + ipmi_ip + "' does not appear to be a valid IPMI IP")
        sys.exit(1)

    mylog.info("Powering off node " + node_ip)
    try:
        libsf.IpmiCommand(ipmi_ip, ipmi_user, ipmi_pass, "chassis power off")
    except libsf.SfError as e:
        mylog.error(str(e))
        sys.exit(1)

    mylog.info("Waiting for " + node_ip + " to go down")
    while (libsf.Ping(node_ip)): time.sleep(1)

    mylog.passed(node_ip + " powered off successfully")


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

