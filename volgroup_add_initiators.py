#!/usr/bin/env python2.7

"""
This action will add a list of initiator IDs (IQN or WWN) to a volume access group
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, ItemList, NameOrID, IPv4AddressType, StrType, OptionalValueType, SolidFireIDType, BoolType
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "initiators" : (ItemList(StrType), None),
    "volgroup_name" : (OptionalValueType(StrType), None),
    "volgroup_id" : (OptionalValueType(SolidFireIDType), None),
    "strict" : (BoolType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def AddInitiatorsToVolgroup(initiators,
                            volgroup_name,
                            volgroup_id,
                            strict,
                            mvip,
                            username,
                            password):
    """
    Add list of initiators to volume access group

    Args:
        initiators:     the list of initiator IDs to add (IQN or WWN)
        volgroup_name:  the name of the group
        volgroup_id:    the ID of the group
        strict:         fail if any of the initiators are already in the group
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    # Validate args
    NameOrID(volgroup_name, volgroup_id, "volume group")

    # Find the group
    log.info("Searching for volume groups")
    try:
        volgroup = SFCluster(mvip, username, password).FindVolumeAccessGroup(volgroupName=volgroup_name, volgroupID=volgroup_id)
    except UnknownObjectError as e:
        log.error(e)
        return False
    except SolidFireError as e:
        log.error("Failed to search for group: {}".format(e))
        return False

    extra = set(initiators).intersection(volgroup.initiators)
    if strict and extra:
        log.error("Some initiators are already in group: [{}]".format(",".join(extra)))
        return False

    # Add the initiators
    log.info("Adding [{}] to group {}".format(", ".join(initiators), volgroup.name))
    try:
        volgroup.AddInitiators(initiators)
    except SolidFireError as e:
        log.error("Failed to modify group: {}".format(e))
        return False

    # Make sure they got added
    missing = set(initiators).difference(volgroup.initiators)
    if missing:
        log.error("Some initiators are missing from the group after modifying volume access group: {}".format(",".join(missing)))
        return False

    log.passed("Successfully added initiators to group {}".format(volgroup.name))
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--initiators", type=ItemList(str), metavar="ID1,ID2...", required=True, help="the list of initiator IDs (IQN, WWN) to add")
    parser.add_argument("--strict", action="store_true", default=False, help="fail if any initiators are already in the group")
    parser.add_volgroup_selection_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(AddInitiatorsToVolgroup, args)
    app.Run(**args)
