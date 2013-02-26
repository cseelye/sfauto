#!/usr/bin/python

# This script will make sure the ensemble is the correct size

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

ssh_user = "root"               # The SSH username for the nodes
                                # --node_user

ssh_pass = "password"         # The SSH password for the nodes

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import re
import libsf
from libsf import mylog


def main():
    global mvip, username, password, ssh_user, ssh_pass

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
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)

    mylog.info("Checking the MVIP")
    if not libsf.Ping(mvip):
        mylog.error("Cannot ping " + mvip)
        sys.exit(1)
    
    # Find the nodes in the cluster
    node_list = dict()
    result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
    for node in result["nodes"]:
        node_list[node["cip"]] = node["mip"]
    node_count = len(node_list.keys())
    mylog.info("Found " + str(node_count) + " nodes in the cluster (" + ",".join(sorted(node_list.values())) + ")")

    # Get the ensemble list
    result = libsf.CallApiMethod(mvip, username, password, 'GetClusterInfo', {})
    ensemble_nodes = sorted(result["clusterInfo"]["ensemble"])
    ensemble_count = len(ensemble_nodes)
    mylog.info("Found " + str(ensemble_count) + " nodes in the ensemble (" + ",".join(ensemble_nodes) + ")")
    
    # Make sure we have the correct number of ensemble members
    if node_count < 3 and ensemble_count != 1: # Less then 3 node, ensemble of 1
        mylog.error("Found " + str(ensemble_count) + " ensemble nodes but expected 1")
        sys.exit(1)
    elif node_count < 5 and ensemble_count != 3: # 3-4 nodes, ensemble of 3
        mylog.error("Found " + str(ensemble_count) + " ensemble nodes but expected 3")
        sys.exit(1)
    elif node_count >= 5 and ensemble_count != 5: #  5 or more nodes, ensemble of 5
        mylog.error("Found " + str(ensemble_count) + " ensemble nodes but expected 5")
        sys.exit(1)

    # Make sure we can connect to and query all of the ensemble servers
    for node_ip in sorted(node_list.values()):
        mylog.info("Connecting to " + node_ip)
        ssh = libsf.ConnectSsh(node_ip, ssh_user, ssh_pass)
        for cip in ensemble_nodes:
            mylog.info("  Checking ZK server at " + cip)
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "zkCli.sh -server " + cip + " get /ensemble; echo $?")
            lines = stdout.readlines();
            if (int(lines.pop().strip()) != 0):
                mylog.error("Could not query zk server on " + cip)
                for line in stderr.readlines():
                    mylog.error(line.rstrip())
                sys.exit(1)
    
    mylog.info("Getting the ensemble report")
    ensemble_report = libsf.HttpRequest("https://" + mvip + "/reports/ensemble", username, password)
    if "error" in ensemble_report:
        m = re.search("<pre>(x\S+)", ensemble_report)
        if m:
            if "xRecvTimeout" in m.group(1):
                mylog.warning("xRecvTimeout but ensemble looks otherwise healthy")
                sys.exit(0)
            mylog.error("Ensemble error detected: " + m.group(1))
        else:
            mylog.error("Ensemble error detected")
        sys.exit(1)
    
    mylog.passed("Ensemble is healthy")


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

