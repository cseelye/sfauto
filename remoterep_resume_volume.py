#!/usr/bin/env python2.7

"""
This action will resume replication on a list of volumes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, StrType, IPv4AddressType, BoolType, OptionalValueType, SolidFireIDType, ItemList, PositiveIntegerType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError, UnknownObjectError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
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
def RemoteRepResumeVolume(volume_names,
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
    Resume replication on a list of volumes

    Args:
        volume_names:       list of volume names to select
        volume_ids:         list of volume IDs to select
        volume_prefix:      select volumes whose names start with this prefix
        volume_regex:       select volumes whose names match this regex
        volume_count:       only select this many volumes
        source_account:     select volumes from this account
        source_account_id:  select volumes from this account
        test:               show the volumes that would be selected but don't actually do anything
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)

    log.info("Searching for volumes")
    try:
        match_volumes = cluster.SearchForVolumes(volumeID=volume_ids, volumeName=volume_names, volumeRegex=volume_regex, volumePrefix=volume_prefix, accountName=source_account, accountID=source_account_id, volumeCount=volume_count)
    except UnknownObjectError:
        match_volumes = {}
    except SolidFireError as e:
        log.error("Failed to search for volumes: {}".format(e))
        return False

    replicating_volumes = [vol for vol in match_volumes.itervalues() if "volumePairs" in vol and vol["volumePairs"]]

    if len(replicating_volumes) <= 0:
        log.warning("No matching volumes")
        return True

    log.info("{} volumes will be modified: {}".format(len(replicating_volumes), ",".join(sorted([vol["name"] for vol in replicating_volumes]))))

    if test:
        log.warning("Test option set; volumes will not be modified")
        return True

    log.info("Modifying volumes...")
    pool = threadutil.GlobalPool()
    results = []
    for volume in replicating_volumes:
        log.info("  Resuming volume {}".format(volume["name"]))
        results.append(pool.Post(_APICallThread, mvip, username, password, volume["volumeID"]))

    allgood = True
    for idx, volume in enumerate(replicating_volumes):
        try:
            
            results[idx].Get()
        except SolidFireError as e:
            log.error("  Error resuming volume {}: {}".format(volume["name"], e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully resumed all volumes")
        return True
    else:
        log.error("Could not resume all volumes")
        return False

@threadutil.threadwrapper
def _APICallThread(mvip, username, password, volume_id):
    """Modify a volume pair, run as a thread"""
    SFCluster(mvip, username, password).ModifyVolumePair(volume_id, {"pausedManual" : False})


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_volume_search_args("to be paused")
    args = parser.parse_args_to_dict()

    app = PythonApp(RemoteRepResumeVolume, args)
    app.Run(**args)
