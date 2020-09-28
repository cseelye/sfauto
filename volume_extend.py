#!/usr/bin/env python2.7

"""
This action will increase the size of a list of volumes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import logargs
from libsf.util import ValidateAndDefault, IPv4AddressType, OptionalValueType, ItemList, SolidFireIDType, PositiveIntegerType, PositiveNonZeroIntegerType, BoolType, RegexType, StrType, SolidFireVolumeSizeType
from libsf import sfdefaults
import copy
from volume_modify import VolumeModify

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "new_size" : (PositiveNonZeroIntegerType, None),
    "gib" : (BoolType, False),
    "volume_names" : (OptionalValueType(ItemList(StrType, allowEmpty=True)), None),
    "volume_ids" : (OptionalValueType(ItemList(SolidFireIDType, allowEmpty=True)), None),
    "volume_prefix" : (OptionalValueType(StrType), None),
    "volume_regex" : (OptionalValueType(RegexType), None),
    "volume_count" : (OptionalValueType(PositiveIntegerType), None),
    "source_account" : (OptionalValueType(StrType), None),
    "source_account_id" : (OptionalValueType(SolidFireIDType), None),
    "test" : (BoolType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def VolumeExtend(new_size,
                 gib,
#pylint: disable=unused-argument
                 volume_names,
                 volume_ids,
                 volume_prefix,
                 volume_regex,
                 volume_count,
                 source_account,
                 source_account_id,
                 test,
                 mvip,
                 username,
                 password):
#pylint: enable=unused-argument
    """
    Increase the size of a list of volumes

    Args:
        new_size:           the new size of the volumes, must be larger than the old size
        gib:                volume size in GiB instead of GB
        volume_names:       list of volume names to select
        volume_ids:         list of volume IDs to select
        volume_prefix:      select volumes whose names start with this prefix
        volume_regex:       select volumes whose names match this regex
        volume_count:       only select this many volumes
        source_account:     select volumes from this account
        source_account_id:  select volumes from this account
        test:               show the volumes that would be selected but don't actually do anything
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    options = copy.deepcopy(locals())
    options.pop("new_size", None)
    options.pop("gib", None)

    if gib:
        multiplier = 1024 * 1024 * 1024
    else:
        multiplier = 1000 * 1000 * 1000

    new_size = new_size * multiplier
    post_value = new_size
    if new_size % 4096 != 0:
        post_value = int((new_size // 4096 + 1) * 4096)

    return VolumeModify(property_name="totalSize",
                         property_value=new_size,
                         post_value=post_value,
                         **options)

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--new-size", type=SolidFireVolumeSizeType(), required=True, metavar="SIZE", help="the volume size in GB")
    parser.add_argument("--gib", action="store_true", default=False, help="volume size in GiB instead of GB")
    parser.add_volume_search_args("to be modified")
    args = parser.parse_args_to_dict()

    app = PythonApp(VolumeExtend, args)
    app.Run(**args)
