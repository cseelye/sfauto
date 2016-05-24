#!/usr/bin/env python2.7

"""
This action will create a volume access group. Optionally add IQNs and volumes to the group while creating
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateArgs, ItemList, IPv4AddressType
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError

@logargs
def CreateVolumeGroup(volgroup_name,
                      iqns=None,
                      volume_names=None,
                      volume_ids=None,
                      volume_prefix=None,
                      volume_regex=None,
                      volume_count=0,
                      source_account=None,
                      source_account_id=None,
                      test=False,
                      strict=False,
                      mvip=sfdefaults.mvip,
                      username=sfdefaults.username,
                      password=sfdefaults.password):
    """
    Create a volume access group

    Args:
        volgroup_name:      the name of the new group
        iqns:               the client iSCSI IQNs to add to the group
        volume_names:       list of volume names to add to the group
        volume_ids:         list of volume IDs to add to the group
        volume_prefix:      add volumes whose names start with this prefix to the group
        volume_regex:       add volumes whose names match this regex to the group
        volume_count:       only add this many volumes to the group
        source_account:     add volumes from this account to the group
        source_account_id:  add volumes from this account to the group
        test:               show the group that would be created but don't actually do it
        strict:             fail if the group already exists
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    # Validate args
    ValidateArgs(locals(), {
        "volgroup_name" : None,
        "mvip" : IPv4AddressType,
        "username" : None,
        "password" : None
    })

    cluster = SFCluster(mvip, username, password)

    # Find the requested volumes
    add_volume_ids = None
    if volume_names or volume_ids or volume_prefix or volume_regex or source_account or source_account_id:
        log.info("Searching for volumes")
        try:
            found_volumes = cluster.SearchForVolumes(volumeID=volume_ids,
                                                     volumeName=volume_names,
                                                     volumeRegex=volume_regex,
                                                     volumePrefix=volume_prefix,
                                                     accountName=source_account,
                                                     accountID=source_account_id,
                                                     volumeCount=volume_count)
        except SolidFireError as e:
            log.error(str(e))
            return False
        add_volume_ids = found_volumes.keys()
        add_volume_names = [found_volumes[i]["name"] for i in add_volume_ids]

    # See if the group already exists
    log.info("Searching for volume groups")
    try:
        cluster.FindVolumeAccessGroup(volgroupName=volgroup_name)
        if strict or iqns or add_volume_ids:
            log.error("Group already exists")
            return False
        else:
            log.passed("Group already exists")
            return True
    except UnknownObjectError:
        # Group does not exist
        pass
    except SolidFireError as e:
        log.error("Could not search for volume groups: {}".format(e))
        return False

    log.info("Creating volume access group '{}'".format(volgroup_name))
    if iqns:
        log.info("  IQNs: {}".format(",".join(iqns)))
    if add_volume_ids:
        log.info("  Volumes: {}".format(",".join(add_volume_names)))

    if test:
        log.info("Test option set; group will not be created")
        return True

    # Create the group
    try:
        cluster.CreateVolumeGroup(volgroup_name, iqns, add_volume_ids)
    except SolidFireError as e:
        log.error("Failed to create group: {}".format(e))
        return False

    log.passed("Successfully created group {}".format(volgroup_name))
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--volgroup-name", type=str, required=True, metavar="NAME", help="the name for the new group")
    parser.add_argument("--iqns", type=ItemList(str), metavar="IQN1,IQN2...",  help="list of initiator IQNs to add to the group")
    parser.add_volume_search_args("to optionally add to the group")
    parser.add_argument("--strict", action="store_true", default=False, help="fail if the group already exists")
    args = parser.parse_args_to_dict()

    app = PythonApp(CreateVolumeGroup, args)
    app.Run(**args)
