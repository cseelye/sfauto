#!/usr/bin/python

# This script get the IPMI IP address from a node

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

node_ip = "192.168.000.000"     # The management VIP of the node to reboot
                                # --node_ip

ssh_user = "root"               # The username for the nodes
                                # --ssh_user

ssh_pass = "password"         # The password for the nodes
                                # --ssh_pass

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
    global node_ip, ssh_user, ssh_pass, csv, bash

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--node_ip", type="string", dest="node_ip", default=node_ip, help="the management IP of the node to reboot")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up. [%default]")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up, [%default]")
    parser.add_option("--csv", action="store_true", dest="csv", help="display a minimal output that is suitable for piping into other programs")
    parser.add_option("--bash", action="store_true", dest="bash", help="display a minimal output that is formatted for a bash array/for loop")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    node_ip = options.node_ip
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    if options.csv:
        csv = True
        mylog.silence = True
    if options.bash:
        bash = True
        mylog.silence = True
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(node_ip):
        mylog.error("'" + node_ip + "' does not appear to be a valid IP")
        sys.exit(1)

    mylog.info("Determining IPMI IP address for " + node_ip)
    ipmi_ip = libsf.GetIpmiIp(node_ip, ssh_user, ssh_pass)
    
    if csv or bash:
        sys.stdout.write(ipmi_ip + "\n")
        sys.stdout.flush()
    else:
        mylog.info("Node " + node_ip + " IPMI IP address is " + ipmi_ip)


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

