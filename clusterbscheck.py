#!/usr/bin/python

"""
This action will run ClusterBSCheck and check the results

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS

    --timeout           How long to wait for ClusterBSCheck to finish (min)
"""

import sys
from optparse import OptionParser
import multiprocessing
import time
import re
import lib.libsf as libsf
from lib.libsf import mylog
import traceback
import datetime
import calendar
import logging
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class ClusterbscheckAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _NodeThread(self, node_ip, node_user, node_pass, results, index):
        try:
            mylog.debug(node_ip + ": Connecting")
            ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)
            #mylog.info(node_ip + ": Checking for xUnknownBlockID")
            command = "grep xUnknownBlock /var/log/sf-slice.info | egrep -o 'bs [0-9]+ .+ sliceID=[0-9]+ lba=[0-9]+' | sort -u"

            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
            results[index] = []
            for line in stdout.readlines():
                m = re.search(r"bs\s+(\d+).+data=\[\"(\S+).+sliceID=(\d+)\s+lba=(\d+)", line)
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
        except KeyboardInterrupt:
            results[index] = False
            return
        except Exception:
            mylog.error(traceback.format_exc())
            self.RaiseFailureEvent(message=str(e), nodeIP=node_ip, exception=e)
            results[index] = False

    def _WaitForBsCheckThread(self, node_ip, node_user, node_pass, ss_count, start_time, timeout, results, index):
        results[index] = False
        try:
            ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)
            # Wait for BS check to start
            command = "grep -c 'Starting ClusterBSCheck' /var/log/sf-slice.info"
            while True:
                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
                if int(stdout.readlines()[0].strip()) >= ss_count:
                    break
                time.sleep(2)

            # Wait for BS check to finish
            mylog.info(node_ip + ": waiting for ClusterBSCheck to finish")
            command = "grep -c 'finished cluster bs check' /var/log/sf-slice.info"
            while True:
                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
                if int(stdout.readlines()[0].strip()) >= ss_count:
                    break
                time.sleep(10)

            mylog.info(node_ip + ": ClusterBSCheck finished")
            results[index] = True
            return

            #pattern = re.compile("^(?P<month>[a-zA-Z]{3})\s+(?P<day>\d\d?)\s(?P<hour>\d\d)\:(?P<minute>\d\d):(?P<second>\d\d)(?:\s(?P<suppliedhost>[a-zA-Z0-9_-]+))?\s(?P<host>[a-zA-Z0-9_-]+)\s(?P<process>[a-zA-Z0-9\/_-]+)(\[(?P<pid>\d+)\])?:\s(?P<message>.+)$")
            #finished = False
            #while not finished:
            #    stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
            #    for line in stdout.readlines():
            #        m = pattern.match(line)
            #        if m:
            #            datestring = m.group('month') + " " + ("%02d" % int(m.group('day'))) + " " + m.group('hour') + ":" + m.group('minute') + ":" + m.group('second')
            #            date = datetime.datetime.strptime(datestring, "%b %d %H:%M:S")
            #            timestamp = calendar.timegm(date_obj.timetuple())
            #            if timestamp >= start_time:
            #                results[index] = True
            #                return
        except KeyboardInterrupt:
            results[index] = False
        except Exception as e:
            mylog.error(node_ip + ": " + str(e))
            self.RaiseFailureEvent(message=str(e), nodeIP=node_ip, exception=e)
            results[index] = False

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "timeout" : libsf.IsInteger},
            args)

    def Execute(self, mvip=sfdefaults.mvip, timeout=90, username=sfdefaults.username, password=sfdefaults.password, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Run ClusterBSCheck and check the results
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Getting a list of nodes in the cluster")
        node_ip_list = []
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + str(e))
            return False
        for node in result["nodes"]:
            node_ip_list.append(node["mip"])

        node2ss = dict()
        result = libsf.CallApiMethod(mvip, username, password, "ListServices", {})
        for service in result["services"]:
            if service["service"]["serviceType"] != "slice": continue
            node_ip = service["node"]["mip"]
            if node_ip in node2ss:
                node2ss[node_ip] += 1
            else:
                node2ss[node_ip] = 1

        for node_ip in node_ip_list:
            mylog.info("Rotating logs on " + node_ip)
            ssh = libsf.ConnectSsh(node_ip, ssh_user, ssh_pass)
            libsf.ExecSshCommand(ssh, "/usr/sbin/logrotate -f /etc/logrotate.d/sfapp")
            ssh.close()
        time.sleep(10)

        mylog.info("Starting ClusterBSCheck")
        start_time = time.time()
        time.sleep(2)
        try:
            result = libsf.CallApiMethod(mvip, username, password, "StartClusterBSCheck", {}, ApiVersion=5.0)
        except libsf.SfError as e:
            mylog.error("Failed to get start BS check: " + str(e))
            return False

        mylog.info("Waiting for check to finish")
        manager = multiprocessing.Manager()
        results = manager.dict()
        current_threads = []
        thread_index = 0
        for node_ip in node_ip_list:
            results[thread_index] = False
            th = multiprocessing.Process(target=self._WaitForBsCheckThread, name="Node-" + node_ip + "-" + str(thread_index), args=(node_ip, ssh_user, ssh_pass, node2ss[node_ip], start_time, timeout, results, thread_index))
            th.start()
            current_threads.append(th)
            thread_index += 1
        # Wait for all threads to stop
        for th in current_threads:
            th.join()
        time.sleep(20)

        mylog.info("Looking for xUnknownBlockID")
        found_xubid = False

        for node_ip in node_ip_list:
            ssh = libsf.ConnectSsh(node_ip, ssh_user, ssh_pass)
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "grep xUnknownBlockID /var/log/sf-block.info | grep -vi Recycling | wc -l")
            if int(stdout.readlines()[0].strip()) > 0:
                mylog.error("Found xUnknownBlockID on " + node_ip)
                found_xubid = True
            ssh.close()

        if found_xubid:
            return False
        else:
            mylog.passed("No xUnknownBlockID found")
            return True


        ## Start one thread per node
        #mylog.info("Gathering information from nodes")
        #manager = multiprocessing.Manager()
        #results = manager.dict()
        #current_threads = []
        #thread_index = 0
        #for node_ip in node_ip_list:
        #    results[thread_index] = False
        #    th = multiprocessing.Process(target=self._NodeThread, name="Node-" + node_ip + "-" + str(thread_index), args=(node_ip, ssh_user, ssh_pass, results, thread_index))
        #    th.start()
        #    current_threads.append(th)
        #    thread_index += 1
        #    #if thread_index > 1: break
        #
        ## Wait for all threads to stop
        #for th in current_threads:
        #    th.join()
        #
        #mylog.debug("Getting a list of accounts in the cluster")
        #account_map = dict()
        #try:
        #    result = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
        #except libsf.SfError as e:
        #    mylog.error("Failed to get account list: " + str(e))
        #    return False
        #for account in result["accounts"]:
        #    acc_id = account["accountID"]
        #    acc_name = account["username"]
        #    account_map[acc_id] = acc_name
        #
        #mylog.debug("Getting a list of volumes in the cluster")
        #vol_map = dict()
        #try:
        #    result = libsf.CallApiMethod(mvip, username, password, "ListActiveVolumes", {})
        #except libsf.SfError as e:
        #    mylog.error("Failed to get volume list: " + str(e))
        #    return False
        #for vol in result["volumes"]:
        #    vol_name = vol["name"]
        #    vol_id = vol["volumeID"]
        #    acc_id = vol["accountID"]
        #    vol_map[vol_id] = dict()
        #    vol_map[vol_id]["volumeID"] = vol_id
        #    vol_map[vol_id]["accountID"] = acc_id
        #    vol_map[vol_id]["volumeName"] = vol_name
        #    vol_map[vol_id]["accountName"] = account_map[acc_id]
        #
        ## Collate the results
        #broken_volumes = dict()
        #error = False
        #for res in results.values():
        #    if res == False:
        #        error = True
        #    else:
        #        for block in res:
        #            slice_id = block["sliceID"]
        #            if slice_id not in broken_volumes:
        #                broken_volumes[slice_id] = []
        #            broken_volumes[slice_id].append(block)
        #
        #for volume_id, block_list in broken_volumes.iteritems():
        #    volume_name = vol_map[volume_id]["volumeName"]
        #    account_name = vol_map[volume_id]["accountName"]
        #    account_id = vol_map[volume_id]["acountID"]
        #    mylog.error("xUnknownBlockID on volume " + volume_name + " (" + volume_id + ") from account " + account_name + " (" + account_id + ")")
        #    for block in block_list:
        #        mylog.error("  blockID " + block["blockID"] + " from BS " + block["bs"] + " for LBA " + block["lba"])
        #
        #if len(broken_volumes) <= 0 and not error:
        #    mylog.passed("No xUnknownBlockID found")
        #    return True
        #else:
        #    return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--timeout", type="int", dest="timeout", default=90, help="how long to wait for ClusterBSCheck to run (min)")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.timeout, options.username, options.password, options.ssh_user, options.ssh_pass, options.debug):
            sys.exit(0)
        else:
            sys.exit(1)
    except libsf.SfArgumentError as e:
        mylog.error("Invalid arguments - \n" + str(e))
        sys.exit(1)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        Abort()
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)

