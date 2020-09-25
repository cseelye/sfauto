#!/usr/bin/env python2.7

"""
This action will delete a list of volumes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateArgs, IPv4AddressType, OptionalValueType, BoolType, ItemList, SolidFireIDType, PositiveIntegerType
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError

@logargs
def VolumeDelete(volume_names=None,
                  volume_ids=None,
                  volume_prefix=None,
                  volume_regex=None,
                  volume_count=0,
                  source_account=None,
                  source_account_id=None,
                  test=False,
                  purge=False,
                  mvip=sfdefaults.mvip,
                  username=sfdefaults.username,
                  password=sfdefaults.password):
    """
    Delete volumes from the cluster

    Args:
        volume_names:       list of volume names to add to the group
        volume_ids:         list of volume IDs to add to the group
        volume_prefix:      add volumes whose names start with this prefix to the group
        volume_regex:       add volumes whose names match this regex to the group
        volume_count:       only add this many volumes to the group
        source_account:     add volumes from this account to the group
        source_account_id:  add volumes from this account to the group
        test:               show the volumes that would be added but don't actually do it
        purge:              purge the deleted volumes
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
        "purge" : BoolType,
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

    # Get a list of volumes to delete
    log.info("Searching for volumes")
    try:
        match_volumes = cluster.SearchForVolumes(volumeID=volume_ids, volumeName=volume_names, volumeRegex=volume_regex, volumePrefix=volume_prefix, accountName=source_account, accountID=source_account_id, volumeCount=volume_count)
    except UnknownObjectError:
        match_volumes = {}
    except SolidFireError as e:
        log.error("Failed to search for volumes: {}".format(e))
        return False

    if len(list(match_volumes.keys())) <= 0:
        log.warning("No matching volumes")
        return True

    log.info("{} volumes will be deleted: {}".format(len(list(match_volumes.keys())), ",".join(sorted([vol["name"] for vol in match_volumes.values()]))))

    if test:
        log.warning("Test option set; volumes will not be deleted")
        return True

    log.info("Deleting {} volumes...".format(len(list(match_volumes.keys()))))
    try:
        cluster.DeleteVolumes(volumeIDs=list(match_volumes.keys()), purge=purge)
    except SolidFireError as e:
        log.error("Failed to delete volumes: {}".format(e))
        return False

    log.passed("Successfully deleted {} volumes".format(len(list(match_volumes.keys()))))
    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_volume_search_args("to delete")
    parser.add_argument("--purge", action="store_true", default=False, help="purge the volumes after deletion")
    args = parser.parse_args_to_dict()

    app = PythonApp(VolumeDelete, args)
    app.Run(**args)
