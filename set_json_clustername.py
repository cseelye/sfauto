#!/usr/bin/python

# This script will set the clustername in solidfire.json on a list of nodes

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

node_ips = [                # The IP addresses of the nodes
    "192.168.000.000",      # --node_ips
]

ssh_user = "root"           # The SSH username for the nodes
                            # --node_user

ssh_pass = "password"     # The SSH password for the nodes

cluster_name = ""           # The cluster name to set in solidfire.json
                            # --cluster_name

force = False               # Ignore safeguards and edit the file anyway
                            # --force

rtfi = False                # Update the RTFI backup version (/sf/rtfi/conf/solidfire.json)
                            # instead of the live version (/etc/solidfire.json)
                            # --rtfi

# ----------------------------------------------------------------------------


import sys
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
    global node_ips, ssh_user, ssh_pass, cluster_name, force, rtfi

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
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--cluster_name", type="string", dest="cluster_name", default=cluster_name, help="the name of the cluster to set in solidfire.json")
    parser.add_option("--force", action="store_true", dest="force", help="ignore safeguards and edit the file anyway")
    parser.add_option("--rtfi", action="store_true", dest="rtfi", help="update RTFI backup file instead of live file")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    cluster_name = options.cluster_name
    if options.force:
        force = True
    if options.rtfi:
        rtfi = True
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


    json_filename = "/etc/solidfire.json"
    if rtfi:
        json_filename = "/sf/rtfi/conf/solidfire.json"

    for node_ip in node_ips:
        # Connect to the node
        mylog.info("Connecting to " + node_ip)
        ssh = libsf.ConnectSsh(node_ip, ssh_user, ssh_pass)

        # Download and parse json file from this node
        mylog.debug("Fetching " + json_filename)
        local_fh, local_filename = tempfile.mkstemp(dir='.', text=True)
        sftp = ssh.open_sftp()
        sftp.get(json_filename, local_filename)
        infile = open(local_filename, 'r')
        sf_json = None
        try:
            sf_json = json.load(infile)
        except ValueError as e:
            mylog.error("Invalid JSON: " + str(e))
            sys.exit(1)
        infile.close()

        if sf_json["cluster"] == cluster_name:
            mylog.passed(node_ip + " is already in cluster " + cluster_name)
            os.unlink(local_filename)
            continue

        if not rtfi:
            mylog.info("Stopping solidfire on " + node_ip)
            libsf.ExecSshCommand(ssh, "stop solidfire")

        # Quick sanity check
        if "nodeID" in sf_json.keys() and sf_json["nodeID"] > 0:
            if force:
                mylog.warning(node_ip + " appears to be in a cluster")
            else:
                mylog.error(node_ip + " appears to be in a cluster")
                os.unlink(local_filename)
                sys.exit(1)

        # Remove the semsemble list if it exists
        if "ensemble" in sf_json.keys():
            mylog.warning(node_ip + " was a pending node in cluster " + sf_json["cluster"])
            del sf_json["ensemble"]

        # Remove the nodeID if it exists
        if "nodeID" in sf_json: del sf_json["nodeID"]

        # Update the cluster name and write it out to file
        mylog.info("Setting cluster name to '" + cluster_name + "' on " + node_ip)
        sf_json["cluster"] = cluster_name
        outfile = open(local_filename, 'w')
        json.dump(sf_json, outfile, indent=8, sort_keys=True)
        outfile.close()

        # Upload the file back to the node and restart solidfire
        sftp.put(local_filename, json_filename)
        sftp.close()

        if not rtfi:
            # Start up solidfire
            mylog.info("Starting solidfire on " + node_ip)
            libsf.ExecSshCommand(ssh, "start solidfire")
            ssh.close()
            # Wait a little while to make sure solidfire is running and discovering other nodes
            time.sleep(15)

        # remove the local file
        os.unlink(local_filename)
        mylog.passed("Set cluster name on " + node_ip)

    mylog.passed("Set cluster name on all nodes")



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

