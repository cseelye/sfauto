#!/usr/bin/python

"""
This action will clone volumes

Specify a single source volume with volume_name/volume_id, or multiple source volumes with source_account/source_account_id/volume_prefix/volume_regex/volume_count
Specify a single clone name with clone_name, or use clone_prefix to have a unique name generated for each clone
Use total_job_count/volume_job count to control how many clones are created in parallel

When run as a script, the following options/env variables apply:
    --mvip                  The managementVIP of the cluster
    SFMVIP env var

    --user                  The cluster admin username
    SFUSER env var

    --pass                  The cluster admin password
    SFPASS env var

    --volume_name           The name of the volume to clone

    --volume_id             The volumeID of the volume to clone

    --volume_prefix         Prefix for the volumes to clone

    --volume_regex          Regex to match volumes to clone

    --volume_count          The max number of volumes to clone (0 for all matches)

    --source_account        Account to use to search for volumes to clone

    --source_account_id     Account to use to search for volumes to clone

    --account_name          The name of the account to create the clones for (assign clones to)

    --account_id            The account ID to create the clones for

    --clone_count           The number of clones to make per volume

    --clone_prefix          Prefix for the clone. Name will be generated as volume_name + clone_prefix + "%05d"

    --clone_name            The name to give to the clone

    --access                Access level for the clones

    --clone_size            The new size for the clone

    --total_job_count       The total number of clone jobs to run in parallel on the cluster

"""

import sys
from optparse import OptionParser
import time
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CloneVolumeAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "access" : lambda x: x in sfdefaults.all_volume_acess_levels
                            },
            args)

    def Execute(self, mvip=sfdefaults.mvip, volume_name=None, volume_id=0, volume_prefix=None, volume_regex=None, volume_count=0, source_account=None, source_account_id=0, account_name=None, account_id=0, clone_count=0, clone_prefix="-c", clone_name=None, access="readWrite", clone_size=0, total_job_count=12, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Clone selected volumes
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Get the list of volumes to clone
        mylog.info("Searching for source volumes")
        try:
            volumes_to_clone = libsf.SearchForVolumes(mvip, username, password, VolumeId=volume_id, VolumeName=volume_name, VolumeRegex=volume_regex, VolumePrefix=volume_prefix, AccountName=source_account, AccountId=source_account_id, VolumeCount=volume_count)
        except libsf.SfError as e:
            mylog.error(e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Find the destination account
        # if account_id > 0:
        #     account_name = None
        # if account_name or account_id > 0:
        #     mylog.info("Searching for destination account")
        #     try:
        #         libsf.FindAccount(mvip, username, password, AccountName=account_name, AccountId=account_id)

        #     except libsf.SfError as e:
        #         mylog.error(str(e))
        #         self.RaiseFailureEvent(message=str(e), exception=e)
        #         return False


        # Find the destination account
        if account_id > 0:
            account_name = None
        if account_name or account_id > 0:
            mylog.info("Searching for destination account")
        try:
            account_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
        except libsf.SfError as e:
            mylog.error("Could not get a list of accounts on " + mvip)

        for account in account_list["accounts"]:
            if account["username"] == account_name:
                account_id = account["accountID"]

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
                    dest_clone_name = source_volume_name + clone_prefix + "%05d" % clone_num
                else:
                    dest_clone_name = clone_name
                mylog.debug("Creating clone job for volume " + source_volume_name + " to clone " + dest_clone_name)
                job = CloneJob()
                job.SourceVolumeName = source_volume_name
                job.SourceVolumeId = source_volume_id
                job.CloneName = dest_clone_name
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
            try:
                result = libsf.CallApiMethod(mvip, username, password, "CloneVolume", params)
            except libsf.SfError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            job.CloneJobHandle = result["asyncHandle"]
            clones_in_progress[job.CloneName] = job
            job_index += 1

            if job_index >= len(clone_jobs):
                # All jobs are started
                break

            # If we have hit job count, wait for a job to finish before starting another
            while len(clones_in_progress.keys()) >= total_job_count:
                for dest_clone_name, job, in clones_in_progress.items():
                    params = {}
                    params["asyncHandle"] = job.CloneJobHandle
                    try:
                        result = libsf.CallApiMethod(mvip, username, password, "GetAsyncResult", params)
                    except libsf.SfError as e:
                        mylog.error(str(e))
                        self.RaiseFailureEvent(message=str(e), exception=e)
                        return False
                    if result["status"].lower() == "complete":
                        if "result" in result:
                            mylog.passed("  Clone " + dest_clone_name + " finished")
                        else:
                            mylog.error("  Clone " + dest_clone_name + " failed -- " + result["error"]["name"] + ": " + result["error"]["message"])
                            failure = True
                        del clones_in_progress[dest_clone_name]
                        break
                if len(clones_in_progress.keys()) >= total_job_count:
                    time.sleep(2)

        # Wait for all remaining jobs to complete
        while len(clones_in_progress.keys()) > 0:
            for dest_clone_name, job, in clones_in_progress.items():
                params = {}
                params["asyncHandle"] = job.CloneJobHandle
                try:
                    result = libsf.CallApiMethod(mvip, username, password, "GetAsyncResult", params)
                except libsf.SfError as e:
                    mylog.error(str(e))
                    return False
                if result["status"].lower() == "complete":
                    if "result" in result:
                        mylog.passed("  Clone " + dest_clone_name + " finished")
                    else:
                        mylog.error("  Clone " + dest_clone_name + " failed -- " + result["error"]["name"] + ": " + result["error"]["message"])
                        failure = True
                        self.RaiseFailureEvent(message="Clone " + dest_clone_name + " failed -- " + result["error"]["name"] + ": " + result["error"]["message"])
                    del clones_in_progress[dest_clone_name]
                    break
            time.sleep(2)

        if failure:
            mylog.error("Not all clones were successful")
            return False
        else:
            mylog.passed("All clones complete")
            return True

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
    parser.add_option("--volume_name", type="string", dest="volume_name", default=None, help="the volume to clone.  If there are more than one with the same name, the first one is selected")
    parser.add_option("--volume_id", type="int", dest="volume_id", default=0, help="the volume to clone.")
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=None, help="the prefix of volumes to clone.  All volume names that start with this string will be cloned")
    parser.add_option("--volume_regex", type="string", dest="volume_regex", default=None, help="regex to search for volumes to delete")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=0, help="the max number of volumes to clone")
    parser.add_option("--source_account", type="string", dest="source_account", default=None, help="the name of the account to select source volumes from")
    parser.add_option("--source_account_id", type="int", dest="source_account_id", default=0, help="the account to select source volumes from")
    parser.add_option("--account_name", type="string", dest="account_name", default=None, help="the account to create the clones for")
    parser.add_option("--account_id", type="int", dest="account_id", default=0, help="the account to create the clones for")
    parser.add_option("--clone_count", type="int", dest="clone_count", default=0, help="the number of clones to create per volume")
    parser.add_option("--clone_prefix", type="string", dest="clone_prefix", default="-c", help="the prefix for the clones (clone name will be volume name + clone prefix + %05d)")
    parser.add_option("--clone_name", type="string", dest="clone_name", default=None, help="the name to give to the clone")
    parser.add_option("--access", type="choice", dest="access", choices=sfdefaults.all_volume_acess_levels, default=sfdefaults.volume_access, help="the access level for the clones (readOnly, readWrite, locked)")
    parser.add_option("--clone_size", type="int", dest="clone_size", default=0, help="the new size for the clone (0 or not specified will keep the same size as the source)")
    parser.add_option("--total_job_count", type="int", dest="total_job_count", default=12, help="the total number of clone jobs to start in parallel across the cluster")
    #parser.add_option("--volume_job_count", type="int", dest="volume_job_count", default=2, help="the number of clone jobs to start in parallel on each volume")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.volume_name, options.volume_id, options.volume_prefix, options.volume_regex, options.volume_count, options.source_account, options.source_account_id, options.account_name, options.account_id, options.clone_count, options.clone_prefix, options.clone_name, options.access, options.clone_size, options.total_job_count, options.username, options.password, debug=options.debug):
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
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

