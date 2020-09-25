#!/usr/bin/env python2.7

"""
This action will force a whole file sync on the given volumes
"""
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateArgs, IPv4AddressType, OptionalValueType, ItemList, SolidFireIDType, PositiveIntegerType, BoolType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError

@logargs
def VolumeForceWholeSync(volume_names=None,
                         volume_ids=None,
                         volume_prefix=None,
                         volume_regex=None,
                         volume_count=0,
                         source_account=None,
                         source_account_id=None,
                         test=False,
                         wait=False,
                         mvip=sfdefaults.mvip,
                         username=sfdefaults.username,
                         password=sfdefaults.password):
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

    # Validate args
    allargs = ValidateArgs(locals(), {
        "volume_names" : OptionalValueType(ItemList(str, allowEmpty=True)),
        "volume_ids" : OptionalValueType(ItemList(SolidFireIDType, allowEmpty=True)),
        "volume_prefix" : OptionalValueType(str),
        "volume_regex" : OptionalValueType(str),
        "volume_count" : OptionalValueType(PositiveIntegerType),
        "source_account" : OptionalValueType(str),
        "source_account_id" : OptionalValueType(SolidFireIDType),
        "test" : BoolType,
        "wait" : BoolType,
        "mvip" : IPv4AddressType,
        "username" : None,
        "password" : None
    })
    # Update locals now that they are validated and typed
    for argname in allargs.keys():
        #pylint: disable=exec-used
        exec("{argname} = allargs['{argname}']".format(argname=argname)) in globals(), locals()
        #pylint: enable=exec-used

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
