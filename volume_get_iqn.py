#!/usr/bin/env python2.7

"""
This action will print the IQN of a volume
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, NameOrID, IPv4AddressType, OptionalValueType, SelectionType, SolidFireIDType, StrType
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError
import sys

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "volume_name" : (OptionalValueType(StrType), None),
    "volume_id" : (OptionalValueType(SolidFireIDType), None),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "output_format" : (OptionalValueType(SelectionType(sfdefaults.all_output_formats)), None),
})
def GetVolumeIQN(volume_name,
                 volume_id,
                 mvip,
                 username,
                 password,
                 output_format):
    """
    Print the volume IQN

    Args:
        volume_name:    the name of the volume
        volume_id:      the ID of the volume
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()
    NameOrID(volume_name, volume_id, "volume")

    log.info("Searching for volume")
    try:
        match = SFCluster(mvip, username, password).SearchForVolumes(volumeID=volume_id, volumeName=volume_name)
    except UnknownObjectError:
        log.error("Could not find volume")
        return False
    except SolidFireError as e:
        log.error("Failed to search for volume: {}".format(e))
        return False
    if len(list(match.keys())) <= 0:
        log.error("Could not find volume")
        return False
    volume = list(match.values())[0]

    # Display the list in the requested format
    if output_format and output_format == "bash":
        sys.stdout.write(volume["iqn"] + "\n")
        sys.stdout.flush()
    elif output_format and output_format == "json":
        sys.stdout.write('{{"iqn" : "{}"}}\n'.format(volume["iqn"]))
    else:
        log.info("Volume {} has IQN {}".format(volume["name"], volume["iqn"]))

    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_single_volume_selection_args()
    parser.add_console_format_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(GetVolumeIQN, args)
    app.Run(**args)
