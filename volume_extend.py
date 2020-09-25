#!/usr/bin/env python2.7

"""
This action will increase the size of a list of volumes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import logargs
from libsf.util import ValidateArgs, IPv4AddressType, SolidFireVolumeSizeType, OptionalValueType, ItemList, SolidFireIDType, PositiveIntegerType, BoolType
from libsf import sfdefaults
import copy
from volume_modify import VolumeModify

@logargs
def VolumeExtend(new_size,
                 gib=False,
#pylint: disable=unused-argument
                 volume_names=None,
                 volume_ids=None,
                 volume_prefix=None,
                 volume_regex=None,
                 volume_count=0,
                 source_account=None,
                 source_account_id=None,
                 test=False,
                 mvip=sfdefaults.mvip,
                 username=sfdefaults.username,
                 password=sfdefaults.password):
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

    # Validate args
    allargs = ValidateArgs(locals(), {
        "new_size" : SolidFireVolumeSizeType(gib),
        "volume_names" : OptionalValueType(ItemList(str, allowEmpty=True)),
        "volume_ids" : OptionalValueType(ItemList(SolidFireIDType, allowEmpty=True)),
        "volume_prefix" : OptionalValueType(str),
        "volume_regex" : OptionalValueType(str),
        "volume_count" : OptionalValueType(PositiveIntegerType),
        "source_account" : OptionalValueType(str),
        "source_account_id" : OptionalValueType(SolidFireIDType),
        "test" : BoolType,
        "mvip" : IPv4AddressType,
        "username" : None,
        "password" : None
    })
    # Update locals now that they are validated and typed
    for argname in allargs.keys():
        #pylint: disable=exec-used
        exec("{argname} = allargs['{argname}']".format(argname=argname)) in globals(), locals()
        #pylint: enable=exec-used

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
