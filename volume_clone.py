#!/usr/bin/env python2.7

"""
This action will create clones of volumes

Specify a single source volume with volume_name/volume_id, or multiple source volumes with source_account/source_account_id/volume_prefix/volume_regex/volume_count
Specify a single clone name with clone_name, or use clone_prefix to have a unique name generated for each clone
Use total_job_count/volume_job_count to control how many clones are created in parallel
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, StrType, IPv4AddressType, PositiveNonZeroIntegerType, PositiveIntegerType, OptionalValueType, ItemList, BoolType, SolidFireIDType, SelectionType
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError
from libsf import threadutil
import time

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "clone_count" : (PositiveNonZeroIntegerType, None),
    "clone_prefix" : (StrType, "-c"),
    "clone_name" : (OptionalValueType(StrType), None),
    "access" : (SelectionType(sfdefaults.all_volume_access_levels), sfdefaults.volume_access),
    "clone_size" : (OptionalValueType(PositiveNonZeroIntegerType), None),
    "total_job_count" : (PositiveNonZeroIntegerType, 12),
    "volume_job_count" : (OptionalValueType(PositiveNonZeroIntegerType), None),
    "dest_account_name" : (OptionalValueType(StrType), None),
    "dest_account_id" : (OptionalValueType(SolidFireIDType), None),
    "volume_names" : (OptionalValueType(ItemList(StrType, allowEmpty=True)), None),
    "volume_ids" : (OptionalValueType(ItemList(SolidFireIDType, allowEmpty=True)), None),
    "volume_prefix" : (OptionalValueType(StrType), None),
    "volume_regex" : (OptionalValueType(StrType), None),
    "volume_count" : (OptionalValueType(PositiveIntegerType), None),
    "source_account" : (OptionalValueType(StrType), None),
    "source_account_id" : (OptionalValueType(SolidFireIDType), None),
    "test" : (BoolType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def VolumeClone(clone_count,
                clone_prefix,
                clone_name,
                access,
                clone_size,
                total_job_count,
                volume_job_count,
                dest_account_name,
                dest_account_id,
                volume_names,
                volume_ids,
                volume_prefix,
                volume_regex,
                volume_count,
                source_account,
                source_account_id,
                test,
                mvip,
                username,
                password):
    """
    Clone a list of volumes

    Args:
        clone_count:            the number of clones to create per volume
        clone_prefix:           the prefix to use to generate clone names (name will be volumeName + clone_prefix + "%05d")
        clone_name:             the name to use for all the clones
        access:                 the access leve for the clones
        clone_size:             the size of the clones (0 to keep the volume size)
        total_job_count:        the total number of clones to start in parallel on the cluster
        volume_job_count:       the number of clones to start in parallel on each volume
        dest_account_name:      the name of the account to clone to, leave off to clone to the same account
        dest_account_id:        the ID of the account to clone to, leave off to clone to the same account
        volume_names:           list of volume names to select
        volume_ids:             list of volume IDs to select
        volume_prefix:          select volumes whose names start with this prefix
        volume_regex:           select volumes whose names match this regex
        volume_count:           only select this many volumes
        source_account:         select volumes from this account
        source_account_id:      select volumes from this account
        test:                   show the volumes that would be selected but don't actually do anything
        mvip:                   the management IP of the cluster
        username:               the admin user of the cluster
        password:               the admin password of the cluster
    """

    log = GetLogger()

    cluster = SFCluster(mvip, username, password)

    # Figure out the max number of volume clones in parallel
    if not volume_job_count or volume_job_count <= 0:
        try:
            volume_job_count = cluster.GetLimits()["cloneJobsPerVolumeMax"]
        except SolidFireError as e:
            log.error("Failed to get cluster limits: {}".format(e))
            return False

    # Find the dest account
    dest_account = None
    if dest_account_name or dest_account_id:
        log.info("Searching for accounts")
        try:
            dest_account = SFCluster(mvip, username, password).FindAccount(accountName=dest_account_name, accountID=dest_account_id)
        except UnknownObjectError:
            log.error("Account does not exist")
            return False
        except SolidFireError as e:
            log.error("Could not search for accounts: {}".format(e))
            return False

    # Get a list of volumes
    log.info("Searching for volumes")
    try:
        match_volumes = cluster.SearchForVolumes(volumeID=volume_ids, volumeName=volume_names, volumeRegex=volume_regex, volumePrefix=volume_prefix, accountName=source_account, accountID=source_account_id, volumeCount=volume_count)
    except SolidFireError as e:
        log.error("Failed to search for volumes: {}".format(e))
        return False

    msg = "{} clones per volume of {} volumes will be created".format(clone_count, len(list(match_volumes.keys())))
    if dest_account:
        msg += " in account {}".format(dest_account.username)
    log.info(msg)
    if test:
        log.info("Test option set; no volumes will be cloned")
        return True

    pool = threadutil.ThreadPool(maxThreads=total_job_count)
    results = []
    allgood = True
    jobs_pervol = min(clone_count, volume_job_count)
    
    def MakeCloneOpts(vol, dest_name):
        opts = {
            "volumeID" : vol["volumeID"],
            "volumeName" : vol["name"],
            "cloneName" : dest_name,
            "access" : access}
        if clone_size and clone_size > 0:
            opts["newSize"] = clone_size
        if dest_account:
            opts["newAccountID"] = dest_account.ID
        return opts
    def WaitForCloneResults(result_list):
        success = True
        for res, volume in result_list:
            try:
                res.Get()
            except SolidFireError as e:
                log.error("  Error cloning volume {}: {}".format(volume["name"], e))
                success = False
                continue
        return success

    queued_clones_pervol = 0
    # Queue up clones for each volume
    for idx in range(1, clone_count+1, jobs_pervol):
        for clone_num in range(idx, idx + jobs_pervol):
            for vol in match_volumes.values():
                # queue up a clone job for this volume
                new_clone_name = clone_name or "{}{}{:05d}".format(vol["name"], clone_prefix, clone_num)
                results.append((pool.Post(_CloneVolume, mvip, username, password, MakeCloneOpts(vol, new_clone_name)), vol))
            # Make sure we don't go over on the last iteration if clone_count/jobs_pervol is not an even number
            queued_clones_pervol += 1
            if queued_clones_pervol >= clone_count:
                break

        # Wait for jobs to complete so we don't go over jobs_pervol
        if not WaitForCloneResults(results):
            allgood = False
        results = []

    if allgood:
        log.passed("Successfully cloned all volumes")
        return True
    else:
        log.error("Not all clones were successfull")
        return False


@threadutil.threadwrapper
def _CloneVolume(mvip, username, password, clone_options):
    """Clone a volume and wait for completion, run as a thread"""
    log = GetLogger()
    volume_name = clone_options.pop("volumeName")
    clone_name = clone_options["cloneName"]
    log.info("  Cloning volume {} to {}".format(volume_name, clone_name))

    cluster = SFCluster(mvip, username, password)
    handle = cluster.CloneVolume(**clone_options)

    while True:
        result = cluster.GetAsyncResult(handle)
        if result["status"].lower() == "complete":
            if "result" in result:
                log.info("  Clone {} finished".format(clone_name))
                break
            elif "error" in result:
                raise SolidFireError("  Clone {} failed {}: {}".format(clone_name, result["error"]["name"], result["error"]["message"]))
            else:
                raise SolidFireError("  Unexpected result: {}".format(result))
        time.sleep(sfdefaults.TIME_SECOND * 5)

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--clone-count", type=PositiveNonZeroIntegerType, required=True, metavar="COUNT", help="the number of clones to create per volume")
    parser.add_argument("--clone-prefix", type=str, default="-c", help="the prefix for the clones (clone name will be volume name + clone prefix + %%05d)")
    parser.add_argument("--clone-name", type=str,  default=None, help="the name to give to the clone")
    parser.add_argument("--access", type=str, choices=sfdefaults.all_volume_access_levels, default=sfdefaults.volume_access, help="the access level for the clones (readOnly, readWrite, locked)")
    parser.add_argument("--clone-size", type=PositiveNonZeroIntegerType, help="the new size for the clone (0 or not specified will keep the same size as the source)")
    parser.add_account_selection_args(required=False, prefix="dest")
    parser.add_argument("--total-job-count", type=int, default=12, metavar="COUNT", help="the total number of clone jobs to start in parallel across the cluster")
    parser.add_argument("--volume-job-count", type=int, metavar="COUNT", help="the number of clone jobs to start in parallel on each volume")
    parser.add_volume_search_args("to be cloned")
    args = parser.parse_args_to_dict()

    app = PythonApp(VolumeClone, args)
    app.Run(**args)

