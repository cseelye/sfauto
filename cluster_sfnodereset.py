#!/usr/bin/env python

# This script will run sfnodereset on multiple nodes in parallel

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

save_logs = True                # Save a copy of the sf logs before reset
                                # override with --nosave_logs

# ----------------------------------------------------------------------------

import sys, os
import multiprocessing
from multiprocessing import Queue
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


def NodeThread(node_ip, node_user, node_pass, save_logs, results, index, debug=None):
    if debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    try:
        mylog.info(node_ip + ": Connecting")
        ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "hostname")
        hostname = stdout.readlines()[0].strip()

        if save_logs:
            archive_name = "sflogs_sfnodereset_" + libsf.TimestampToStr(time.time(), "%Y-%m-%d-%H-%M-%S") + ".tgz"
            mylog.info(node_ip + ": Saving sf-* logs to /var/log/" + archive_name)
            libsf.ExecSshCommand(ssh, "cd /var/log&&tar czf " + archive_name + " sf-*")

        mylog.info(node_ip + ": Starting sfnodereset")
        libsf.ExecSshCommand(ssh, "nohup /sf/bin/sfnodereset -fR > sfnr.out 2>&1 &")
        time.sleep(5)
        ssh.close();

        time.sleep(20)
        mylog.info(node_ip + ": Waiting for node to go down")
        # Wait for the node to go down
        wait_start = time.time()
        while(libsf.Ping(node_ip)):
            time.sleep(2)
            if time.time() - wait_start > 60 * 7: # See if it's been longer than 7 minutes
                mylog.warning(node_ip + ": Taking too long; aborting")
                results[index] = False
                return

        mylog.info(node_ip + ": Waiting for node to reboot")
        # Wait for the node to come back up
        while(not libsf.Ping(node_ip)):
            time.sleep(10)

        mylog.info(node_ip + ": Node is back up")
        results[index] = True
    except Exception as e:
        mylog.error(str(e))
        results[index] = False

def main():
    global node_ips, ssh_user, ssh_pass, save_logs

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
    parser.add_option("--node_ips", type="string", dest="node_ips", default=node_ips, help="the IP addresses of the nodes: ie. 192.168.133.47,192.168.133.48")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--nosave_logs", action="store_true", dest="nosave_logs", help="do not save a copy of sf logs before reset")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    debug = options.debug
    try:
        node_ips = libsf.ParseIpsFromList(options.node_ips)
    except TypeError as e:
        mylog.error(e)
        sys.exit(1)
    if not node_ips:
        mylog.error("Please supply at least one node IP address")
        sys.exit(1)
    if options.nosave_logs:
        save_logs = False
    if debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)


    # Start one thread per node
    manager = multiprocessing.Manager()
    results = manager.dict()
    current_threads = []
    thread_index = 0
    for node_ip in node_ips:
        results[thread_index] = False
        th = multiprocessing.Process(target=NodeThread, args=(node_ip, ssh_user, ssh_pass, save_logs, results, thread_index, debug))
        th.start()
        current_threads.append(th)
        thread_index += 1

    # Wait for all threads to stop
    for th in current_threads:
        th.join()

    # Check the results
    all_success = True
    for res in results.values():
        if not res:
            all_success = False
    if all_success:
        mylog.passed("Successfully reset all nodes")
        sys.exit(0)
    else:
        mylog.error("Could not reset all nodes")
        sys.exit(1)


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
