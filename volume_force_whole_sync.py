#!/usr/bin/env python2.7

"""
This action will force a whole file sync on the given volumes
"""
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, OptionalValueType, ItemList, SolidFireIDType, PositiveIntegerType, BoolType, StrType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError

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
    "wait" : (BoolType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def VolumeForceWholeSync(volume_names,
                         volume_ids,
                         volume_prefix,
                         volume_regex,
                         volume_count,
                         source_account,
                         source_account_id,
                         test,
                         wait,
                         mvip,
                         username,
                         password):
    """
    Force whole file sync on the volumes

    Args:
        volume_names:       list of volume names to add to the group
        volume_ids:         list of volume IDs to add to the group
        volume_prefix:      add volumes whose names start with this prefix to the group
        volume_regex:       add volumes whose names match this regex to the group
        volume_count:       only add this many volumes to the group
        source_account:     add volumes from this account to the group
        source_account_id:  add volumes from this account to the group
        test:               show the volumes that would be added but don't actually do it
        wait:               wait for syncing to complete
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)

    # Get a list of volumes
    log.info("Searching for volumes")
    try:
        match_volumes = cluster.SearchForVolumes(volumeID=volume_ids,
                                                 volumeName=volume_names,
                                                 volumeRegex=volume_regex,
                                                 volumePrefix=volume_prefix,
                                                 accountName=source_account,
                                                 accountID=source_account_id,
                                                 volumeCount=volume_count)
    except SolidFireError as e:
        log.error("Failed to search for volumes: {}".format(e))
        return False

    if not match_volumes:
        log.passed("No matching volumes were found")
        return True
    log.info("{} volumes selected: {}".format(len(list(match_volumes.keys())),
                                                   ",".join(sorted([vol["name"] for vol in match_volumes.values()]))))
    if test:
        log.warning("Test option set; no action will be taken")
        return True

    pool = threadutil.GlobalPool()
    results = []
    for volume_id in match_volumes.keys():
        results.append(pool.Post(_VolumeThread, mvip, username, password, volume_id, wait))

    allgood = True
    for idx, volume_id in enumerate(match_volumes.keys()):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  Error syncing volume {}: {}".format(match_volumes[volume_id]["name"], e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully synced all volumes")
        return True
    else:
        log.error("Could not sync all volumes")
        return False

@threadutil.threadwrapper
def _VolumeThread(mvip, username, password, volume_id, wait):
    """Force syncing on a volume"""
    log = GetLogger()
    log.info("Forcing whole file sync on volume {}".format(volume_id))
    SFCluster(mvip, username, password).ForceWholeFileSync(volume_id, wait)


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_volume_search_args("to force syncing on")
    parser.add_argument("--wait", action="store_true", default=False, help="wait for syncing to complete")
    args = parser.parse_args_to_dict()

    app = PythonApp(VolumeForceWholeSync, args)
    app.Run(**args)
