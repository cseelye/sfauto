#!/usr/bin/python

# This script will clone a volume or volumes a given number of times

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"    # The management VIP of the cluster
                            # --mvip

username = "admin"          # Admin account for the cluster
                            # --user

password = "password"      # Admin password for the cluster
                            # --pass

volume_name = ""            # The name of the volume to clone
                            # If volume_id > 0 is specified, it will be used instead of volume_name
                            # Either volume_name or volume_id must be specified
                            # --volume_name

volume_id = 0               # The volumeID of the volume to clone
                            # --volume_id

volume_prefix = ""          # Prefix for the volumes to clone
                            # volume_name or volume_id will supercede this
                            # --volume_prefix

volume_regex = ""           # Regex to match volumes to clone
                            # --volume_regex

volume_count = 0            # The max number of volumes to clone (o for all matches)
                            # --volume_count

source_account = ""         # Account to use to search for volumes to clone
                            # Can be used with volume_prefix
                            # volume_name or volume_id will supercede this
                            # --source_account

account_name = None         # The name of the account to create the clones for (assign clones to)
                            # If account_id > 0 is specified, it will be used instead of account_name
                            # Either account_name or account_id must be specified
                            # --account_name

account_id = 0              # The account ID to create the clones for
                            # Values <= 0 will be ignored and account_name will be used instead
                            # Either account_name or account_id must be specified
                            # --account_id

clone_count = 1             # The number of clones to make per volume
                            # --clone_count

clone_prefix = "-c"         # Prefix for the clone. Name will be generated as volume_name + clone_prefix + "%05d"
                            # --volume_prefix

clone_name = ""             # The name to give to the clone
                            # --clone_name

access = "readWrite"        # Access level for the clones
                            # --access

clone_size = 0              # The new size for the clone
                            # 0 or None will keep the same size as the source
                            # --clone_size

total_job_count = 8         # The total number of clone jobs to run in parallel on the cluster
                            # Limited to 2 per slice service in Be
                            # --total_job_count

volume_job_count = 2        # The number of clone jobs to run in parallel on each volume
                            # Limited to 2 in Be
                            # --volume_job_count

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog, SfError


def main():
    global mvip, username, password, account_name, account_id, clone_prefix, clone_name, clone_count, access, clone_size, total_job_count, volume_job_count, source_account, volume_id, volume_name, volume_id, volume_prefix, volume_regex, volume_count

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
    parser.add_option("--volume_name", type="string", dest="volume_name", default=volume_name, help="the volume to clone.  If there are more than one with the same name, the first one is selected")
    parser.add_option("--volume_id", type="int", dest="volume_id", default=volume_id, help="the volume to clone.")
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=volume_prefix, help="the prefix of volumes to clone.  All volume names that start with this string will be cloned")
    parser.add_option("--volume_regex", type="string", dest="volume_regex", default=volume_regex, help="regex to search for volumes to delete")
    parser.add_option("--source_account", type="string", dest="source_account", default=source_account, help="the name of the account to select source volumes from")
    parser.add_option("--account_name", type="string", dest="account_name", default=account_name, help="the account to create the clones for (either name or id must be specified)")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=volume_count, help="the max number of volumes to clone")
    parser.add_option("--account_id", type="int", dest="account_id", default=account_id, help="the account to create the clones for (either name or id must be specified)")
    parser.add_option("--clone_count", type="int", dest="clone_count", default=clone_count, help="the number of clones to create per volume")
    parser.add_option("--clone_prefix", type="string", dest="clone_prefix", default=clone_prefix, help="the prefix for the clones (clone name will be volume name + clone prefix + %05d)")
    parser.add_option("--clone_name", type="string", dest="clone_name", default=clone_name, help="the name to give to the clone")
    parser.add_option("--access", type="string", dest="access", default=access, help="the access level for the clones (readOnly, readWrite, locked)")
    parser.add_option("--clone_size", type="int", dest="clone_size", default=clone_size, help="the new size for the clone (0 or not specified will keep the same size as the source)")
    parser.add_option("--total_job_count", type="int", dest="total_job_count", default=total_job_count, help="the total number of clone jobs to start in parallel across the cluster")
    parser.add_option("--volume_job_count", type="int", dest="volume_job_count", default=volume_job_count, help="the number of clone jobs to start in parallel on each volume")
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
    account_name = options.account_name
    clone_count = options.clone_count
    clone_prefix = options.clone_prefix
    clone_name = options.clone_name
    clone_size = options.clone_size
    total_job_count = options.total_job_count
    volume_job_count = options.volume_job_count
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)


    # Get the list of volumes to clone
    mylog.info("Searching for source volumes")
    try:
        volumes_to_clone = libsf.SearchForVolumes(mvip, username, password, VolumeId=volume_id, VolumeName=volume_name, VolumeRegex=volume_regex, VolumePrefix=volume_prefix, AccountName=source_account, VolumeCount=volume_count)
    except SfError as e:
        mylog.error(e.message)
        sys.exit(1)

    # Find the destination account
    if (account_id > 0):
        account_name = None
    if account_id > 0 or account_name != None:
        mylog.info("Searching for destination account")
        all_accounts = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
        for account in all_accounts["accounts"]:
            if (account_id <= 0):
                if (account["username"].lower() == account_name.lower()):
                    account_id = account["accountID"]
                    break
            else:
                if (account["accountID"] == account_id):
                    account_name = account["username"]
                    break
        if (account_id <= 0):
            mylog.error("Could not find account with name '" + account_name + "'")
            sys.exit(1)
        if (account_name == None):
            mylog.error("Could not find account ID '" + account_id + "'")
            sys.exit(1)

    class CloneJob:
        def __init__(self):
            self.SourceVolumeName = ""
            self.SourceVolumeId = ""
            self.CloneName = ""
            self.CloneJobHandle = None

    # Create a list of jobs to execute
    clone_jobs = []
    for source_volume_name, source_volume_id in volumes_to_clone.items():
        for clone_num in range(1, clone_count + 1):
            if clone_name == None or len(clone_name) <= 0:
                clone_name = source_volume_name + clone_prefix + "%05d"%clone_num
            job = CloneJob()
            job.SourceVolumeName = source_volume_name
            job.SourceVolumeId = source_volume_id
            job.CloneName = clone_name
            clone_jobs.append(job)

    mylog.info(str(len(volumes_to_clone.keys())) + " volumes to clone")
    mylog.info(str(len(clone_jobs)) + " total clones to create")

    # Start cloning
    failure = False
    clones_in_progress = dict()
    job_index = 0
    while True:
        # Start a new clone
        job = clone_jobs[job_index]
        mylog.info("Starting clone of " + job.SourceVolumeName + " to " + job.CloneName)
        params = {}
        params["volumeID"] = job.SourceVolumeId
        params["name"] = job.CloneName
        params["access"] = access
        if (account_id > 0):
            params["newAccountID"] = account_id
        if clone_size != None and clone_size > 0:
            params["newSize"] = clone_size
        result = libsf.CallApiMethod(mvip, username, password, "CloneVolume", params)
        job.CloneJobHandle = result["asyncHandle"]
        clones_in_progress[job.CloneName] = job
        job_index += 1

        if job_index >= len(clone_jobs):
            # All jobs are started
            break

        # If we have hit job count, wait for a job to finish before starting another
        while len(clones_in_progress.keys()) >= total_job_count:
            for clone_name, job, in clones_in_progress.items():
                params = {}
                params["asyncHandle"] = job.CloneJobHandle
                result = libsf.CallApiMethod(mvip, username, password, "GetAsyncResult", params)
                if result["status"].lower() == "complete":
                    if "result" in result:
                        mylog.passed("  Clone " + clone_name + " finished")
                    else:
                        mylog.error("  Clone " + clone_name + " failed -- " + result["error"]["name"] + ": " + result["error"]["message"])
                        failure = True
                    del clones_in_progress[clone_name]
                    break
            if len(clones_in_progress.keys()) >= total_job_count:
                time.sleep(2)

    # Wait for all remaining jobs to complete
    while len(clones_in_progress.keys()) > 0:
        for clone_name, job, in clones_in_progress.items():
            params = {}
            params["asyncHandle"] = job.CloneJobHandle
            result = libsf.CallApiMethod(mvip, username, password, "GetAsyncResult", params)
            if result["status"].lower() == "complete":
                if "result" in result:
                    mylog.passed("  Clone " + clone_name + " finished")
                else:
                    mylog.error("  Clone " + clone_name + " failed -- " + result["error"]["name"] + ": " + result["error"]["message"])
                    failure = True
                del clones_in_progress[clone_name]
                break
        time.sleep(2)

    if failure:
        mylog.error("Not all clones were successful")
        sys.exit(1)
    else:
        mylog.passed("All clones complete")
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







