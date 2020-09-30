#!/usr/bin/env python

"""
This action will delete a list of volumes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, OptionalValueType, ItemList, SolidFireIDType, PositiveIntegerType, BoolType, StrType
from libsf import sfdefaults
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
    "purge" : (BoolType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def VolumeDelete(volume_names,
                  volume_ids,
                  volume_prefix,
                  volume_regex,
                  volume_count,
                  source_account,
                  source_account_id,
                  test,
                  purge,
                  mvip,
                  username,
                  password):
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
