#!/usr/bin/env python2.7

"""
This action will delete a volume access group
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateArgs, NameOrID, IPv4AddressType
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError

@logargs
def DeleteVolgroup(volgroup_name=None,
                     volgroup_id=0,
                     strict=False,
                     mvip=sfdefaults.mvip,
                     username=sfdefaults.username,
                     password=sfdefaults.password):
    """
    Delete a volume access group

    Args:
        volgroup_name:  the name of the group
        volgroup_id:    the ID of the group
        strict:         fail if the group does not exist
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    # Validate args
    NameOrID(volgroup_name, volgroup_id, "volume group")
    ValidateArgs(locals(), {
        "mvip" : IPv4AddressType,
        "username" : None,
        "password" : None,
    })

    # Find the group
    log.info("Searching for volume groups")
    try:
        volgroup = SFCluster(mvip, username, password).FindVolumeAccessGroup(volgroupName=volgroup_name, volgroupID=volgroup_id)
    except UnknownObjectError as e:
        if strict:
            log.error("Volume group does not exist")
            return False
        else:
            log.passed("Volume group does not exist")
            return True
    except SolidFireError as e:
        log.error("Failed to search for group: {}".format(e))
        return False

    # Delete the group
    try:
        volgroup.Delete()
    except SolidFireError as e:
        log.error("Failed to delete group: {}".format(e))
        return False
    
    try:
        SFCluster(mvip, username, password).FindVolumeAccessGroup(volgroupID=volgroup.ID)
        log.error("Volume group still exists after deleting it")
        return False
    except UnknownObjectError:
        pass

    log.passed("Group deleted successfully")
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__))
    parser.add_cluster_mvip_args()
    parser.add_volgroup_selection_args()
    parser.add_argument("--strict", action="store_true", default=False, help="fail if the group does not exist")
    args = parser.parse_args_to_dict()

    app = PythonApp(DeleteVolgroup, args)
    app.Run(**args)
