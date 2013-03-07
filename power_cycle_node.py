#!/usr/bin/python

# This script will power cycle a cluster node

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

node_ip = "192.168.000.000"     # The management VIP of the node to reboot
                                # --node_ip

ssh_user = "root"               # The username for the nodes
                                # --ssh_user

ssh_pass = "sf.9012182"         # The password for the nodes
                                # --ssh_pass

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
import commands
import re

def main():
    global node_ip, ssh_user, ssh_pass, ipmi_user, ipmi_pass
    env_enabled_vars = [ "ipmi_user", "ipmi_pass" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--node_ip", type="string", dest="node_ip", default=node_ip, help="the management IP of the node to reboot")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up. [%default]")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up, [%default]")
    parser.add_option("--ipmi_user", type="string", dest="ipmi_user", default=ipmi_user, help="the IPMI username for the nodes [%default]")
    parser.add_option("--ipmi_pass", type="string", dest="ipmi_pass", default=ipmi_pass, help="the IPMI password for the nodes [%default]")
    parser.add_option("--nowait", action="store_true", dest="nowait", help="do not wait for the node to come back up")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    node_ip = options.node_ip
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    ipmi_user = options.ipmi_user
    ipmi_pass = options.ipmi_pass
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)

    ssh = libsf.ConnectSsh(node_ip, ssh_user, ssh_pass)
    stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "ipmitool lan print; echo $?")
    ssh.close()
    lines = stdout.readlines()
    if (int(lines.pop().strip()) != 0):
        mylog.error("Failed to run ipmitool - " + "\n".join(stderr.readlines()))
        sys.exit(1)
    ipmi_ip = None
    for line in lines:
        m = re.search("IP Address\s+: (\S+)", line)
        if m:
            ipmi_ip = m.group(1)
            break
    if not ipmi_ip:
        mylog.error("Could not find an IPMI IP address for this node")
        sys.exit(1)
    
    mylog.info("Power cycling " + node_ip)
    retry = 3
    status = None
    output = ""
    while retry > 0:
        status, output = commands.getstatusoutput("ipmitool -Ilanplus -U" + ipmi_user + " -P" + ipmi_pass + " -H" + ipmi_ip + " -E chassis power reset")
        if status == 0:
            break
        retry -= 1
        time.sleep(3)
    
    mylog.info("Waiting for " + node_ip + " to go down")
    while (libsf.Ping(node_ip)): time.sleep(1)

    mylog.info("Waiting for " + node_ip + " to come up")
    time.sleep(120)
    while (not libsf.Ping(node_ip)): time.sleep(1)

    # Wait a couple extra seconds for services to be starting up
    time.sleep(10)

    mylog.passed(node_ip + " power cycled successfully")


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

