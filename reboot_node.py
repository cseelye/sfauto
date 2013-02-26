#!/usr/bin/python

# This script will reboot a cluster node

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

node_ip = "192.168.000.000"         # The management VIP of the node to reboot
                                    # --node_ip

ssh_user = "root"               # The username for the nodes
                                # --ssh_user

ssh_pass = "password"          # The password for the nodes
                                # --ssh_pass

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog


def main():
    # Parse command line arguments
    parser = OptionParser()
    global node_ip, ssh_user, ssh_pass
    parser.add_option("--node_ip", type="string", dest="node_ip", default=node_ip, help="the management IP of the node to reboot")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    node_ip = options.node_ip
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)


    mylog.info("Rebooting " + node_ip)
    ssh = libsf.ConnectSsh(node_ip, ssh_user, ssh_pass)
    libsf.ExecSshCommand(ssh, "shutdown -r now")
    ssh.close()

    mylog.info("Waiting for " + node_ip + " to go down")
    while (libsf.Ping(node_ip)): time.sleep(1)

    mylog.info("Waiting for " + node_ip + " to come up")
    time.sleep(120)
    while (not libsf.Ping(node_ip)): time.sleep(1)

    # Wait a couple extra seconds for services to be started up
    time.sleep(10)

    mylog.passed(node_ip + " rebooted successfully")


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

