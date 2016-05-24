#!/usr/bin/env python2.7

"""
This action will set QoS on a list of volumes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import logargs
from libsf.util import ValidateArgs, IPv4AddressType, OptionalValueType, ItemList, SolidFireIDType, PositiveIntegerType, BoolType, SolidFireBurstIOPSType, SolidFireMaxIOPSType, SolidFireMinIOPSType
from libsf import sfdefaults
import copy
from volume_modify import VolumeModify

@logargs
def VolumeSetQos(min_iops=100,
                 max_iops=15000,
                 burst_iops=15000,
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
    Set QoS on a list of volumes

    Args:
        min_iops:           the min IOPS guarantee for the volumes
        max_iops:           the max sustained IOPS for the volumes
        burst_iops:         the max burst IOPS for the volumes
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

    # Validate args
    allargs = ValidateArgs(locals(), {
        "min_iops" : SolidFireMinIOPSType,
        "max_iops" : SolidFireMaxIOPSType,
        "burst_iops" : SolidFireBurstIOPSType,
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

    options = copy.deepcopy(allargs)
    for key in ["min_iops", "max_iops", "burst_iops"]:
        options.pop(key, None)

    return VolumeModify(property_name="qos",
                         property_value={
                            "minIOPS" : min_iops,
                            "maxIOPS" : max_iops,
                            "burstIOPS" : burst_iops
                         },
                         **options)

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_qos_args()
    parser.add_volume_search_args("to be modified")
    args = parser.parse_args_to_dict()

    app = PythonApp(VolumeSetQos, args)
    app.Run(**args)
