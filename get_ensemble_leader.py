#!/usr/bin/python

# This script will print the ensemble leader node mIP

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "solidfire"              # Admin password for the cluster
                                    # --pass

ssh_user = "root"               # The username for the nodes
                                # --ssh_user

ssh_pass = "password"          # The password for the nodes
                                # --ssh_pass

csv = False                     # Display minimal output that is suitable for piping into other programs
                                # --csv

bash = False                    # Display minimal output that is formatted for a bash array/for  loop
                                # --bash

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
from random import randint
import libsf
from libsf import mylog


def main():
    global mvip, username, password, csv, bash

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
    parser.add_option("--csv", action="store_true", dest="csv", help="display a minimal output that is suitable for piping into other programs")
    parser.add_option("--bash", action="store_true", dest="bash", help="display a minimal output that is formatted for a bash array/for loop")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    if options.csv:
        csv = True
        mylog.silence = True
    if options.bash:
        bash = True
        mylog.silence = True
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    mylog.info("Looking for ensemble lader on cluster " + mvip)

    # Use the first node in the cluster to connect and query each ensemble node until we find the leader
    result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
    ssh = libsf.ConnectSsh(result["nodes"][0]["mip"], ssh_user, ssh_pass)

    # For each ensemble node, query if it is the leader
    leader = None
    cluster_info = libsf.CallApiMethod(mvip, username, password, 'GetClusterInfo', {})
    mylog.info("Ensemble is [" + ", ".join(cluster_info["clusterInfo"]["ensemble"]) + "]")
    for teng_ip in cluster_info["clusterInfo"]["ensemble"]:
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "echo 'mntr' | nc " + teng_ip + " 2181 | grep zk_server_state | cut -f2")
        data = stdout.readlines()[0].strip()
        if data == "leader":
            leader = teng_ip
            break

    if not leader:
        mylog.error("Could not find ensemble leader")
        sys.exit(1)

    if csv or bash:
        mylog.debug("Ensemble leader node is " + leader)
        sys.stdout.write(leader + "\n")
        sys.stdout.flush()
    else:
        mylog.info("Ensemble leader node is " + leader)

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

