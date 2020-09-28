#!/usr/bin/env python2.7

"""
This action will change the LUN assignments for volumes in a volume access group

The method parameter sets how renumbering is done:
    'seq' means number sequentially, starting from min.
    'rev' means number seqentially in reverse, starting from max.
    'rand' means number using values randomly selected between min and max.
    'vol' means to use the volumeID as the LUN number.
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, NameOrID, IPv4AddressType, SelectionType, OptionalValueType, SolidFireIDType, IntegerRangeType, StrType
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError, InvalidArgumentError
import random

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "method" : (SelectionType(sfdefaults.all_numbering_types), sfdefaults.all_numbering_types[0]),
    "lun_min" : (OptionalValueType(IntegerRangeType(0, 16383)), 0),
    "lun_max" : (OptionalValueType(IntegerRangeType(0, 16383)), 16383),
    "volgroup_name" : (OptionalValueType(StrType), None),
    "volgroup_id" : (OptionalValueType(SolidFireIDType), None),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ModifyVolgroupLunAssignments(method,
                                 lun_min,
                                 lun_max,
                                 volgroup_name,
                                 volgroup_id,
                                 mvip,
                                 username,
                                 password):

    """
    Renumber LUNS in the specified volume access group
    """
    log = GetLogger()

    # Validate args
    NameOrID(volgroup_name, volgroup_id, "volume group")
    if lun_max < lun_min:
        raise InvalidArgumentError("lun_min must be <= lun_max")

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

    # Check that the min/max will work for this volume group
    if method == 'seq':
        if lun_min + len(volgroup.volumes) > 16383:
            log.error("min LUN value is too high - volumes will exceed LUN 16383")
            return False
    elif method == 'rev':
        if lun_max - len(volgroup.volumes) < 0:
            log.error("max LUN value is too low - volumes will go below LUN 0")
            return False
    elif method == 'rand':
        if lun_max - lun_min + 1 < len(volgroup["volumes"]):
            log.error("min to max range is too small to fit all volumes in this group")
            return False
    elif method == 'vol':
        if max(volgroup.volumes) > 16383:
            log.error("Max volume ID is too large to use as a LUN number")
            return False

    # Create the new LUN assignments
    lun_assignments = []
    if method == 'seq':
        lun = lun_min
        for volume_id in volgroup.volumes:
            lun_assignments.append({'volumeID' : volume_id, 'lun' : lun})
            lun += 1
    elif method == 'rev':
        lun = lun_max
        for volume_id in volgroup.volumes:
            lun_assignments.append({'volumeID' : volume_id, 'lun' : lun})
            lun -= 1
    elif method == 'rand':
        luns = list(range(lun_min, lun_max+1))
        random.shuffle(luns)
        for i, volume_id in enumerate(volgroup.volumes):
            lun_assignments.append({'volumeID' : volume_id, 'lun' : luns[i]})
    elif method == 'vol':
        for volume_id in volgroup.volumes:
            lun_assignments.append({'volumeID' : volume_id, 'lun' : volume_id})

    # Modify the group
    log.info("Modifying group {}".format(volgroup.name))
    try:
        volgroup.ModifyLUNAssignments(lun_assignments)
    except SolidFireError as e:
        log.error("Failed to modify volume group: {}".format(e))
        return False

    log.passed("Successfully modified LUN assignments in group {}".format(volgroup.name))
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_volgroup_selection_args()
    parser.add_argument("--method", type=str, choices=sfdefaults.all_numbering_types, default='seq', help="The method to use for renumbering")
    parser.add_argument("--min", type=int, dest="lun_min", default=0, help="the smallest LUN number to use")
    parser.add_argument("--max", type=int, dest="lun_max", default=16383, help="the largest LUN number to use")
    args = parser.parse_args_to_dict()

    app = PythonApp(ModifyVolgroupLunAssignments, args)
    app.Run(**args)
