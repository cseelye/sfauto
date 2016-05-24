#!/usr/bin/env python2.7

"""
This action will add volumes to an existing volume access group
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateArgs, IPv4AddressType, NameOrID
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError

@logargs
def AddVolumesToVolgroup(volume_names=None,
                         volume_ids=None,
                         volume_prefix=None,
                         volume_regex=None,
                         volume_count=0,
                         source_account=None,
                         source_account_id=None,
                         volgroup_name=None,
                         volgroup_id=0,
                         test=False,
                         strict=False,
                         mvip=sfdefaults.mvip,
                         username=sfdefaults.username,
                         password=sfdefaults.password):
    """
    Add volumes to a volume access group

    Args:
        volgroup_name:      the name of the group
        volgroup_id:        the ID of the group
        volume_names:       list of volume names to add to the group
        volume_ids:         list of volume IDs to add to the group
        volume_prefix:      add volumes whose names start with this prefix to the group
        volume_regex:       add volumes whose names match this regex to the group
        volume_count:       only add this many volumes to the group
        source_account:     add volumes from this account to the group
        source_account_id:  add volumes from this account to the group
        test:               show the volumes that would be added but don't actually do it
        strict:             fail if there are no volumes to add
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    # Validate args
    NameOrID(volgroup_name, volgroup_id, "volume group")
    ValidateArgs(locals(), {
        "mvip" : IPv4AddressType,
        "username" : None,
        "password" : None
    })

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

    # Get a list of volumes to add
    log.info("Searching for volumes")
    try:
        match_volumes = cluster.SearchForVolumes(volumeID=volume_ids, volumeName=volume_names, volumeRegex=volume_regex, volumePrefix=volume_prefix, accountName=source_account, accountID=source_account_id, volumeCount=volume_count)
    except UnknownObjectError:
        match_volumes = {}
    except SolidFireError as e:
        log.error("Failed to search for volumes: {}".format(e))
        return False

    already_in = list(set(volgroup.volumes).intersection(match_volumes.keys()))
    log.debug("{} total matches, {} volumes are already already in group".format(len(match_volumes.keys()), len(already_in)))

    volumes_to_add = list(set(match_volumes.keys()).difference(volgroup.volumes))

    if len(volumes_to_add) <= 0:
        if strict:
            log.error("No matching volumes were found")
            return False
        else:
            log.passed("No matching volumes were found")
            return True

    log.info("{} volumes will be added to group {}: {}".format(len(volumes_to_add), volgroup.name, ",".join(sorted([match_volumes[vid]["name"] for vid in volumes_to_add]))))

    if test:
        log.warning("Test option set; volumes will not be added")
        return True

    # Add the volumes
    log.info("Adding volumes to group")
    try:
        volgroup.AddVolumes(volumes_to_add)
    except SolidFireError as e:
        log.error("Failed to modify group: " + str(e))
        return False
    
    missing = set(volumes_to_add).difference(volgroup.volumes)
    if missing:
        log.error("Some volumes are missing from group after modifying volume access group: {}".format(",".join(["{{name={} ID={}}}".format(match_volumes[vid]["name"], vid) for vid in missing])))
        return False

    log.passed("Successfully added volumes to group {}".format(volgroup.name))
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_volgroup_selection_args()
    parser.add_volume_search_args("to add to the group")
    parser.add_argument("--strict", action="store_true", default=False, help="fail if the volumes are already in the group")
    args = parser.parse_args_to_dict()

    app = PythonApp(AddVolumesToVolgroup, args)
    app.Run(**args)
