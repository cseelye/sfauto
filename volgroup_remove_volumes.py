#!/usr/bin/env python2.7

"""
This action will remove volumes from an existing volume access group
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, NameOrID, IPv4AddressType, OptionalValueType, ItemList, SolidFireIDType, PositiveIntegerType, BoolType, StrType
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "volgroup_name" : (OptionalValueType(StrType), None),
    "volgroup_id" : (OptionalValueType(SolidFireIDType), None),
    "volume_names" : (OptionalValueType(ItemList(StrType, allowEmpty=True)), None),
    "volume_ids" : (OptionalValueType(ItemList(SolidFireIDType, allowEmpty=True)), None),
    "volume_prefix" : (OptionalValueType(StrType), None),
    "volume_regex" : (OptionalValueType(StrType), None),
    "volume_count" : (OptionalValueType(PositiveIntegerType), None),
    "source_account" : (OptionalValueType(StrType), None),
    "source_account_id" : (OptionalValueType(SolidFireIDType), None),
    "test" : (BoolType, False),
    "strict" : (BoolType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def RemoveVolumesFromVolgroup(volgroup_name,
                              volgroup_id,
                              volume_names,
                              volume_ids,
                              volume_prefix,
                              volume_regex,
                              volume_count,
                              source_account,
                              source_account_id,
                              test,
                              strict,
                              mvip,
                              username,
                              password):
    """
    Remove volumes from a volume access group

    Args:
        volgroup_name:      the name of the group
        volgroup_id:        the ID of the group
        volume_names:       list of volume names to remove
        volume_ids:         list of volume IDs to remove
        volume_prefix:      remove volumes whose names start with this prefix
        volume_regex:       remove volumes whose names match this regex
        volume_count:       only remove this many volumes
        source_account:     remove volumes from this account
        source_account_id:  remove volumes from this account
        test:               show the change that would be made but don't actually do it
        strict:             fail if there are no volumes to remove
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()
    NameOrID(volgroup_name, volgroup_id, "volume group")

    cluster = SFCluster(mvip, username, password)

    # Find the group
    log.info("Searching for volume groups")
    try:
        volgroup = cluster.FindVolumeAccessGroup(volgroupName=volgroup_name, volgroupID=volgroup_id)
    except UnknownObjectError as e:
        log.error(e)
        return False
    except SolidFireError as e:
        log.error("Failed to search for group: {}".format(e))
        return False

    # Get a list of volumes to remove
    log.info("Searching for volumes")
    try:
        match_volumes = cluster.SearchForVolumes(volumeID=volume_ids, volumeName=volume_names, volumeRegex=volume_regex, volumePrefix=volume_prefix, accountName=source_account, accountID=source_account_id, volumeCount=volume_count, volgroupName=volgroup_name, volgroupID=volgroup_id)
    except UnknownObjectError:
        match_volumes = {}
    except SolidFireError as e:
        log.error("Failed to search for volumes: {}".format(e))
        return False

    if len(list(match_volumes.keys())) <= 0:
        if strict:
            log.error("No matching volumes were found in group")
            return False
        else:
            log.passed("No matching volumes were found in group")
            return True

    log.info("{} volumes will be removed from group {}: {}".format(len(list(match_volumes.keys())), volgroup.name, ",".join(sorted([vol["name"] for vol in match_volumes.values()]))))

    if test:
        log.warning("Test option set; volumes will not be removed")
        return True

    # Remove the volumes
    log.info("Removing volumes from group")
    try:
        volgroup.RemoveVolumes(list(match_volumes.keys()))
    except SolidFireError as e:
        log.error("Failed to modify group: " + str(e))
        return False

    # Verify the volumes were removed
    extra = set(match_volumes.keys()).intersection(volgroup.volumes)
    if extra:
        log.error("Some volumes are still in the group after modifying volume access group: {}".format(",".join(extra)))
        return False

    log.passed("Successfully removed volumes from group {}".format(volgroup.name))
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_volgroup_selection_args()
    parser.add_volume_search_args("to remove from the group")
    parser.add_argument("--strict", action="store_true", default=False, help="fail if the volumes are already out of the group")
    args = parser.parse_args_to_dict()

    app = PythonApp(RemoveVolumesFromVolgroup, args)
    app.Run(**args)
