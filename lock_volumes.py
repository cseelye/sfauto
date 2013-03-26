#!/usr/bin/python

# This script will lock the volumes on a cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass

volume_name = ""                # The name of the volume to change
                                # --volume_name

volume_id = 0                   # The volumeID of the volume to change
                                # --volume_id

volume_prefix = ""              # Prefix for the volumes to change
                                # volume_name or volume_id will supercede this
                                # --volume_prefix

volume_regex = ""               # Regex to search for volumes to delete
                                # --volume_regex

volume_count = 0            # The max number of volumes to clone (o for all matches)
                            # --volume_count

source_account = ""             # Account to use to search for volumes to change
                                # Can be used with volume_prefix, volume_regex
                                # volume_name or volume_id will supercede this
                                # --source_account

parallel_thresh = 1             # Do not thread calls unless there are more than this many
                                # --parallel_thresh

parallel_max = 100               # Max number of threads to use
                                # --parallel_max

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import re
import multiprocessing
import libsf
from libsf import mylog

def ApiCallThread(mvip, username, password, volume_name, volume_id, results, index):
    mylog.info("Updating access on " + volume_name)
    params = {}
    params["volumeID"] = volume_id
    params["access"] = "locked"
    try:
        result = libsf.CallApiMethod(mvip, username, password, "ModifyVolume", params, ExitOnError=False)
    except SfApiError as e:
        mylog.error("[" + e.name + "]: " + e.message)
        return
    
    results[index] = True
    return

def main():
    global mvip, username, password, source_account, volume_id, volume_name, volume_id, volume_prefix, volume_regex, volume_count, parallel_thresh, parallel_max

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password", "parallel_thresh", "parallel_max" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--volume_name", type="string", dest="volume_name", default=volume_name, help="the volume to lock")
    parser.add_option("--volume_id", type="int", dest="volume_id", default=volume_id, help="the volume to lock")
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=volume_prefix, help="the prefix of volumes to lock")
    parser.add_option("--volume_regex", type="string", dest="volume_regex", default=volume_regex, help="regex to search for volumes to lock")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=volume_count, help="the max number of volumes to lock")
    parser.add_option("--source_account", type="string", dest="source_account", default=source_account, help="the name of the account to select volumes from")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=parallel_thresh, help="do not thread calls unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=parallel_max, help="the max number of threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    volume_name = options.volume_name
    volume_id = options.volume_id
    volume_prefix = options.volume_prefix
    volume_regex = options.volume_regex
    volume_count = options.volume_count
    source_account = options.source_account
    parallel_thresh = options.parallel_thresh
    parallel_max = options.parallel_max
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    # Get a list of volumes to modify
    mylog.info("Searching for volumes")
    try:
        volumes = libsf.SearchForVolumes(mvip, username, password, VolumeId=volume_id, VolumeName=volume_name, VolumeRegex=volume_regex, VolumePrefix=volume_prefix, AccountName=source_account, VolumeCount=volume_count)
    except SfError as e:
        mylog.error(e.message)
        sys.exit(1)

    # Run the API operations in parallel if there are enough
    if len(volumes.keys()) <= parallel_thresh:
        parallel_calls = 1
    else:
        parallel_calls = parallel_max

    # Start the client threads
    manager = multiprocessing.Manager()
    results = manager.dict()
    current_threads = []
    thread_index = 0
    for volume_name,volume_id in volumes.items():
        results[thread_index] = False
        th = multiprocessing.Process(target=ApiCallThread, args=(mvip, username, password, volume_name, volume_id, results, thread_index))
        th.start()
        current_threads.append(th)
        thread_index += 1

        # Wait for at least one thread to finish
        while len(current_threads) >= parallel_calls:
            for i in range(len(current_threads)):
                if not current_threads[i].is_alive():
                    del current_threads[i]
                    break

    # Wait for all threads to be done
    for th in current_threads:
        th.join()
    # Check the results
    all_success = True
    for res in results.values():
        if not res:
            all_success = False

    if all_success:
        mylog.passed("Successfully locked all volumes")
        sys.exit(0)
    else:
        mylog.error("Could not lock all volumes")
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
