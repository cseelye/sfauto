#!/usr/bin/env python

# This script will save the support bundle from a list of nodes to the local system

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

node_ips = [                    # The IP addresses of the nodes
    "192.168.133.0",            # --node_ips
    "192.168.133.0",
    "192.168.133.0",
    "192.168.133.0",
    "192.168.133.0",
]

ssh_user = "root"               # The username for the nodes
                                # --ssh_user

ssh_pass = "password"          # The password for the nodes
                                # --ssh_pass

folder = "bundles"              # The directory to save the support bundle(s) in
                                # --folder

label = "bundle"                # The label to prepend to the bundle filename
                                # --label

# ----------------------------------------------------------------------------

import sys
import multiprocessing
import os
import time
import re
from optparse import OptionParser
import libsf
from libsf import mylog

try:
    import ssh
except ImportError:
    mylog.warning("Using paramiko module instead of ssh module; this script may have issues with a large number of nodes")

def main():
    global node_ips, ssh_user, ssh_pass, folder, label

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
    parser.add_option("--folder", type="string", dest="folder", default=folder, help="the name of the directory to store the bundle(s) in.  Default is " + str(folder))
    parser.add_option("--label", type="string", dest="label", default=label, help="a label to prepend to the name of the bundle file.  Default is " + str(label))
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    folder = options.folder
    label = options.label
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

    # Function to be run as a worker thread
    def NodeThread(timestamp, node_ip, node_user, node_pass):
        try:
            mylog.info(node_ip + ": Connecting")
            ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "hostname")
            hostname = stdout.readlines()[0].strip()

            # Create a support bundle
            mylog.info(node_ip + ": Generating support bundle")
            bundle_name = label + "_" + timestamp + "_" + hostname + ".tar"
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "/sf/scripts/sf_make_support_bundle " + bundle_name + ";echo $?")
            data = stdout.readlines()
            retcode = int(data.pop())
            if retcode != 0:
                mylog.error(str(stderr.readlines()))
                ssh.close()
                return False

            # Compress the bundle using parallel gzip
            mylog.info(node_ip + ": Compressing bundle")
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "pigz " + bundle_name + ";echo $?")
            data = stdout.readlines()
            retcode = int(data.pop())
            if retcode != 0:
                mylog.error(str(stderr.readlines()))
                ssh.close()
                return False
            bundle_name = bundle_name + ".gz"

            # Copy the file to the local system
            mylog.info(node_ip + ": Saving bundle to " + folder + "/" + bundle_name)
            sftp = ssh.open_sftp()
            sftp.get(bundle_name, folder + "/" + bundle_name)
            sftp.close()

            # Remove the copy on the node
            libsf.ExecSshCommand(ssh, "rm " + bundle_name)
            ssh.close()
            mylog.info(node_ip + ": Finished")
            return True
        except Exception as e:
            mylog.error(str(e))
            return False


    # Create the output directory
    if (not os.path.exists(folder)):
        os.makedirs(folder)

    # Start one thread per node
    threads = []
    timestamp = time.strftime("%Y-%m-%d-%H-%M-%S")
    for node_ip in node_ips:
        # technically these are processes, not threads
        th = multiprocessing.Process(target=NodeThread, args=(timestamp, node_ip, ssh_user, ssh_pass))
        th.start()
        threads.append(th)

    # Wait for all threads to stop
    for th in threads:
        th.join()





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
