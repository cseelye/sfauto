#!/usr/bin/python

# This script will create volumes on an sf cluster for a specific client

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

client_ips = [                      # The IP addresses of the clients
    "192.168.000.000",              # --client_ips
]

client_user = "root"                # The username for the client
                                    # --client_user

client_pass = "password"           # The password for the client
                                    # --client_pass

mvip = "192.168.000.000"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass

volume_count = 20               # The number of volumes to create
                                # --volume_count

volume_size = 0                 # The volume size in GB
                                # --volume_size

enable_512 = False              # Use 512e on the volumes
                                # --512e

max_iops = 15000                # QoS max IOPs
                                # --max_iops

min_iops = 100                  # QoS min IOPs
                                # --min_iops

burst_iops = 15000              # QoS burst IOPs
                                # --burst_iops

wait = 0                        # How long to wait between creating each volume (seconds)
                                # --wait

parallel_thresh = 5             # Do not thread clients unless there are more than this many
                                # --parallel_thresh

parallel_max = 10               # Max number of client threads to use
                                # --parallel_max

# ----------------------------------------------------------------------------


import sys,os
from optparse import OptionParser
import json
import urllib2
import random
import platform
import time
import re
import multiprocessing
import libsf
from libsf import mylog
import libclient
from libclient import ClientError, SfClient


def ClientThread(client_ip, client_user, client_pass, accounts_list, results, index, debug=None):
    if debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    client = SfClient()
    mylog.info(client_ip + ": Connecting to client")
    try:
        client.Connect(client_ip, client_user, client_pass)
    except ClientError as e:
        mylog.error(client_ip + ": " + e.message)
        return

    if client.RemoteOs == libclient.OsType.Windows and not enable_512:
        mylog.warning("512e not enabled - this may cause Windows problems!")

    # Find the corresponding account on the cluster
    account_name = client.Hostname;
    account_id = 0
    for account in accounts_list["accounts"]:
        if account["username"].lower() == account_name.lower():
            account_id = account["accountID"]
            break
    if account_id == 0:
        mylog.error(client_ip + ": Could not find account " + client.Hostname + " on " + mvip)
        return

    # See if there are existing volumes
    volume_start = 0
    params = {}
    params["accountID"] = account_id
    volumes_list = libsf.CallApiMethod(mvip, username, password, "ListVolumesForAccount", params)
    for vol in volumes_list["volumes"]:
        m = re.search("(\d+)$", vol["name"])
        if m:
            vol_num = int(m.group(1))
            if vol_num > volume_start:
                volume_start = vol_num
    volume_start += 1

    # Create the requested volumes
    mylog.info(client_ip + ": Creating " + str(volume_count) + " volumes for account " + account_name)
    volume_prefix = client.Hostname.lower() + "-v"
    for vol_num in range(volume_start, volume_start + volume_count):
        volume_name = volume_prefix + "%05d" % vol_num
        params = {}
        params["name"] = volume_name
        params["accountID"] = account_id
        params["totalSize"] = int(volume_size * 1000 * 1000 * 1000)
        params["enable512e"] = enable_512
        qos = {}
        qos["maxIOPS"] = max_iops
        qos["minIOPS"] = min_iops
        qos["burstIOPS"] = burst_iops
        params["qos"] = qos
        volume_obj = libsf.CallApiMethod(mvip, username, password, "CreateVolume", params)
        mylog.info(client_ip + ":   Created volume " + volume_name)
        if (wait > 0):
            time.sleep(wait)

    mylog.passed(client_ip + ": Successfully created " + str(volume_count) + " volumes for " + client.Hostname)
    results[index] = True
    return

def main():
    global client_ips, client_user, client_pass, mvip, username, password, volume_count, volume_size, enable_512, max_iops, min_iops, burst_iops, wait, parallel_thresh, parallel_max

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password", "client_ips", "client_user", "client_pass", "parallel_thresh", "parallel_max" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]
    if isinstance(client_ips, basestring):
        client_ips = client_ips.split(",")


    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--client_ips", type="string", dest="client_ips", default=",".join(client_ips), help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=client_pass, help="the password for the clients [%default]")
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster  [%default]")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster  [%default]")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=volume_count, help="the number of volumes to create")
    parser.add_option("--volume_size", type="int", dest="volume_size", default=volume_size, help="the volume size in GB")
    parser.add_option("--max_iops", type="int", dest="max_iops", default=max_iops, help="the max sustained IOPS to allow on this volume  [%default]")
    parser.add_option("--min_iops", type="int", dest="min_iops", default=min_iops, help="the min sustained IOPS to guarentee on this volume  [%default]")
    parser.add_option("--burst_iops", type="int", dest="burst_iops", default=burst_iops, help="the burst IOPS to allow on this volume  [%default]")
    parser.add_option("--512e", action="store_true", dest="enable_512", help="use 512 sector emulation")
    parser.add_option("--wait", type="int", dest="wait", default=wait, help="how long to wait between creating each volume (seconds) [%default]")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=parallel_thresh, help="do not thread clients unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=parallel_max, help="the max number of client threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    volume_count = options.volume_count
    volume_size = options.volume_size
    max_iops = options.max_iops
    min_iops = options.min_iops
    burst_iops = options.burst_iops
    enable_512 = options.enable_512
    wait = options.wait
    parallel_thresh = options.parallel_thresh
    parallel_max = options.parallel_max
    debug = options.debug
    if (enable_512 == None):
        enable_512 = False
    client_user = options.client_user
    client_pass = options.client_pass
    try:
        client_ips = libsf.ParseIpsFromList(options.client_ips)
    except TypeError as e:
        mylog.error(e)
        sys.exit(1)
    if not client_ips:
        mylog.error("Please supply at least one client IP address")
        sys.exit(1)
    if debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    # Get a list of accounts from the cluster
    accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})

    # Run the client operations in parallel if there are enough clients
    if len(client_ips) <= parallel_thresh:
        parallel_clients = 1
    else:
        parallel_clients = parallel_max

    # Start the client threads
    manager = multiprocessing.Manager()
    results = manager.dict()
    current_threads = []
    thread_index = 0
    for client_ip in client_ips:
        results[thread_index] = False
        th = multiprocessing.Process(target=ClientThread, args=(client_ip, client_user, client_pass, accounts_list, results, thread_index))
        th.start()
        current_threads.append(th)
        thread_index += 1

        # Wait for at least one thread to finish
        while len(current_threads) >= parallel_clients:
            for i in range(len(current_threads)):
                if not current_threads[i].is_alive():
                    del current_threads[i]
                    break

    # Wait for all threads to stop
    for th in current_threads:
        th.join()
    # Check the results
    all_success = True
    for res in results.values():
        if not res:
            all_success = False

    if all_success:
        mylog.passed("Successfully created volumes for all clients")
        sys.exit(0)
    else:
        mylog.error("Could not create volumes for all clients")
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
