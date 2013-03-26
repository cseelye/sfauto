#!/usr/bin/python

# This script will show details about xUnknownBlockID

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "0.0.0.0"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

ssh_user = "root"               # The username for the nodes
                                # --ssh_user

ssh_pass = "password"          # The password for the nodes
                                # --ssh_pass

timeout = 90                    # How long to wait for ClusterBSCheck to finish, in min
                                # --timeout

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import multiprocessing
import time
import re
import libsf
from libsf import mylog
import logging
import traceback
import datetime
import calendar

def NodeThread(node_ip, node_user, node_pass, results, index):
    #mylog.console.setLevel(logging.DEBUG)
    try:
        mylog.debug(node_ip + ": Connecting")
        #mylog.debug(node_ip + ": index = " + str(index))
        ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)
        #mylog.info(node_ip + ": Checking for xUnknownBlockID")
        command = "zgrep xUnknownBlock /var/log/sf-slice.info* | egrep -o 'bs [0-9]+ .+ sliceID=[0-9]+ lba=[0-9]+' | sort -u"
        
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
        results[index] = []
        for line in stdout.readlines():
            m = re.search("bs\s+(\d+).+data=\[\"(\S+).+sliceID=(\d+)\s+lba=(\d+)", line)
            if (m):
                slice_id = m.group(3)
                lba = m.group(4)
                block = dict()
                block_index = -1
                for i, v in enumerate(results[index]):
                    if v["sliceID"] == slice_id and v["lba"] == lba:
                        block = v
                        block_index = i
                        break
                block["lba"] = lba
                block["sliceID"] = slice_id
                if "bs" not in block:
                    block["bs"] = []
                block["bs"].append(m.group(1))
                block["blockID"] = m.group(2)
                
                if block_index > 0:
                    results[index][block_index] = block
                else:
                    a = results[index]
                    a.append(block)
                    results[index] = a
        
    except Exception as e:
        mylog.error(traceback.format_exc())
        results[index] = False

def WaitForBsCheckThread(node_ip, node_user, node_pass, start_time, timeout, results, index):
    results[index] = False
    try:
        ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)
        command = "grep 'finished cluster bs check' /var/log/sf-slice.info | tail"
        pattern = re.compile("^(?P<month>[a-zA-Z]{3})\s+(?P<day>\d\d?)\s(?P<hour>\d\d)\:(?P<minute>\d\d):(?P<second>\d\d)(?:\s(?P<suppliedhost>[a-zA-Z0-9_-]+))?\s(?P<host>[a-zA-Z0-9_-]+)\s(?P<process>[a-zA-Z0-9\/_-]+)(\[(?P<pid>\d+)\])?:\s(?P<message>.+)$")
        finished = False
        while not finished:
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
            for line in stdout.readlines():
                m = pattern.match(line)
                if m:
                    datestring = m.group('month') + " " + ("%02d" % int(m.group('day'))) + " " + m.group('hour') + ":" + m.group('minute') + ":" + m.group('second')
                    date = datetime.datetime.strptime(datestring, "%b %d %H:%M:S")
                    timestamp = calendar.timegm(date_obj.timetuple())
                    if timestamp >= start_time:
                        results[index] = True
                        return
    except KeyboardInterrupt:
        results[index] = False
    except Exception as e:
        results[index] = False

def main():
    global mvip, username, password, ssh_user, ssh_pass, timeout

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
    parser.add_option("--timeout", type="int", dest="timeout", default=timeout, help="")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    timeout = options.timeout
    timeout = timeout * 60
    
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    mylog.info("Checking cluster version on " + mvip)
    
    result = libsf.CallApiMethod(mvip, username, password, "GetClusterVersionInfo", {})
    if not result["clusterVersion"].startswith("5"):
        mylog.error("This script only works with Boron and later")
        sys.exit(1)
    if float(result["clusterVersion"]) < 5.735:
        mylog.error("This script only works with build 735 or later")
        sys.exit(1)
    
    mylog.info("Getting a list of nodes in the cluster")
    node_ip_list = []
    result = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
    for node in result["nodes"]:
        node_ip_list.append(node["mip"])
    
    mylog.info("Starting ClusterBSCheck")
    start_time = time.time()
    time.sleep(2)
    result = libsf.CallApiMethod(mvip, username, password, "StartClusterBSCheck", {}, ApiVersion=5.0)
    
    mylog.info("Waiting for check to finish")
    manager = multiprocessing.Manager()
    results = manager.dict()
    current_threads = []
    thread_index = 0
    for node_ip in node_ip_list:
        results[thread_index] = False
        th = multiprocessing.Process(target=WaitForBsCheckThread, name="Node-" + node_ip + "-" + str(thread_index), args=(node_ip, ssh_user, ssh_pass, start_time, timeout, results, thread_index))
        th.start()
        current_threads.append(th)
        thread_index += 1
    # Wait for all threads to stop
    for th in current_threads:
        th.join()
    time.sleep(60)
    
    mylog.info("Looking for xUnknownBlockID")
    
    # Start one thread per node
    mylog.info("Gathering information from nodes")
    manager = multiprocessing.Manager()
    results = manager.dict()
    current_threads = []
    thread_index = 0
    for node_ip in node_ip_list:
        results[thread_index] = False
        th = multiprocessing.Process(target=NodeThread, name="Node-" + node_ip + "-" + str(thread_index), args=(node_ip, ssh_user, ssh_pass, results, thread_index))
        th.start()
        current_threads.append(th)
        thread_index += 1
        #if thread_index > 1: break

    # Wait for all threads to stop
    for th in current_threads:
        th.join()

    mylog.debug("Getting a list of accounts in the cluster")
    account_map = dict()
    result = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
    for account in result["accounts"]:
        acc_id = account["accountID"]
        acc_name = account["username"]
        account_map[acc_id] = acc_name
    
    mylog.debug("Getting a list of volumes in the cluster")
    vol_map = dict()
    result = libsf.CallApiMethod(mvip, username, password, "ListActiveVolumes", {})
    for vol in result["volumes"]:
        vol_name = vol["name"]
        vol_id = vol["volumeID"]
        acc_id = vol["accountID"]
        vol_map[vol_id] = dict()
        vol_map[vol_id]["volumeID"] = vol_id
        vol_map[vol_id]["accountID"] = acc_id
        vol_map[vol_id]["volumeName"] = vol_name
        vol_map[vol_id]["accountName"] = account_map[acc_id]

    # Collate the results
    broken_volumes = dict()
    error = False
    for res in results.values():
        if res == False:
            error = True
        else:
            for block in res:
                slice_id = block["sliceID"]
                if slice_id not in broken_volumes:
                    broken_volumes[slice_id] = []
                broken_volumes[slice_id].append(block)
    
    for volume_id, block_list in broken_volumes.iteritems():
        volume_name = vol_map[volume_id]["volumeName"]
        account_name = vol_map[volume_id]["accountName"]
        account_id = vol_map[volume_id]["acountID"]
        mylog.error("xUnknownBlockID on volume " + volume_name + " (" + volume_id + ") from account " + account_name + " (" + account_id + ")")
        for block in block_list:
            mylog.error("  blockID " + block["blockID"] + " from BS " + block["bs"] + " for LBA " + block["lba"])
    
    if len(broken_volumes) <= 0 and not error:
        mylog.passed("No xUnknownBlockID found")
        sys.exit(0)
    else:
        sys.exit(1)


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

