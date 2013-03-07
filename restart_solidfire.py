#!/usr/bin/env python

# This script restart solidfire on multiple nodes simultaneously

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

def NodeThread(node_ip, node_user, node_pass, starter, shared_data, index):
    try:
        mylog.info(node_ip + ": Connecting")
        ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)

        mylog.debug(node_ip + ": Waiting")
        shared_data["ready_count"] += 1
        starter.wait()

        # Quit if all threads were not able to connect
        if shared_data["abort"]: return

        mylog.info(node_ip + ": Restarting solidfire")
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "stop solidfire;start solidfire;echo $?")
        output = stdout.readlines()
        error = stderr.readlines()
        ssh.close();
        retcode = int(output.pop())
        if retcode != 0:
            mylog.error(node_ip + ": Error restarting solidfire: " + "\n".join(error))
            shared_data[index] = False
            return

        mylog.passed(node_ip + ": Successfully restarted solidfire")
        shared_data[index] = True
    except Exception as e:
        mylog.error(str(e))

def main():
    global node_ips, ssh_user, ssh_pass

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
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
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


    # Start one thread per node
    starter = multiprocessing.Event()
    starter.clear()
    manager = multiprocessing.Manager()
    shared_data = manager.dict() # One big shared area for data - lazy I know
    shared_data["ready_count"] = 0
    shared_data["abort"] = False
    threads = []
    thread_index = 0
    for node_ip in node_ips:
        shared_data[thread_index] = False
        th = multiprocessing.Process(target=NodeThread, args=(node_ip, ssh_user, ssh_pass, starter, shared_data, thread_index))
        th.start()
        threads.append(th)
        thread_index += 1

    # Wait for all threads to be connected
    mylog.debug("Waiting for all nodes to be connected")
    while shared_data["ready_count"] < len(threads):
        for th in threads:
            if not th.is_alive():
                mylog.debug("Thread failed; aborting")
                shared_data["abort"] = True
                starter.set()
                time.sleep(1)
                mylog.error("Failed to restart solidfire")
                sys.exit(1)
        time.sleep(0.2)

    mylog.debug("Releasing threads")
    starter.set()

    # Wait for all threads to stop
    for th in threads:
        th.join()

    for i in range(0, len(threads)):
        if not shared_data[i]:
            mylog.error("Failed to restart all nodes")
            sys.exit(1)

    mylog.passed("All nodes restarted")
    sys.exit(0)




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
