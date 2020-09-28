#!/usr/bin/env python2.7

"""
This action will modify a property of a list of volumes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, OptionalValueType, ItemList, IsSet, SolidFireIDType, PositiveIntegerType, BoolType, RegexType, StrType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "property_name" : (StrType, None),
    "property_value" : (IsSet, None),
    "post_value" : (OptionalValueType(None), None),
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
def VolumeModify(property_name,
                 property_value,
                 post_value,
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
    """
    Modify a property on a list of volumes

    Args:
        property_name:      the property to modify
        property_value:     the new value to set
        post_value:         the value to very against after modification. If None, property_value is used
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
    post_value = post_value or property_value
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)

    log.info("Searching for volumes")
    try:
        match_volumes = cluster.SearchForVolumes(volumeID=volume_ids,
                                                 volumeName=volume_names,
                                                 volumeRegex=volume_regex,
                                                 volumePrefix=volume_prefix,
                                                 accountName=source_account,
                                                 accountID=source_account_id,
                                                 volumeCount=volume_count)
    except SolidFireError as e:
        log.error("Failed to search for volumes: {}".format(e))
        return False

    if not match_volumes:
        log.passed("No matching volumes were found")
        return True
    log.info("{} volumes selected: {}".format(len(list(match_volumes.keys())),
                                                   ",".join(sorted([vol["name"] for vol in match_volumes.values()]))))
    if test:
        log.warning("Test option set; no action will be taken")
        return True

    log.info("Modifying volumes...")
    pool = threadutil.GlobalPool()
    results = []
    for volume in match_volumes.values():
        log.info("  Setting {} on volume {}".format(property_name, volume["name"]))
        results.append(pool.Post(_APICallThread, mvip, username, password, volume["volumeID"], property_name, property_value, post_value))

    allgood = True
    for idx, volume in enumerate(match_volumes.values()):
        try:
            
            results[idx].Get()
        except SolidFireError as e:
            log.error("  Error modifying volume {}: {}".format(volume["name"], e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully set {} on all volumes".format(property_name))
        return True
    else:
        log.error("Could not set {} on all volumes".format(property_name))
        return False

@threadutil.threadwrapper
def _APICallThread(mvip, username, password, volume_id, property_name, property_value, post_value):
    """Modify a volume, run as a thread"""

    # Make the change
    vol = SFCluster(mvip, username, password).ModifyVolume(volume_id, {property_name : property_value})

    # Verify that the change was applied
    if isinstance(post_value, dict):
        for key, value in post_value.items():
            if str(vol[property_name][key]) != str(value):
                raise SolidFireError("{} is not correct after modifying volume {} [expected={}, actual={}]".format(key, volume_id, value, vol[property_name][key]))
    else:
        if str(vol[property_name]) != str(post_value):
            raise SolidFireError("{} is not correct after modifying volume {} [expected={}, actual={}]".format(property_name, volume_id, post_value, vol[property_name]))


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--property", dest="property_name", required=True, metavar="NAME", help="The name of the property to set")
    parser.add_argument("--value", dest="property_value", required=True, metavar="VALUE", help="The value of the property")
    parser.add_volume_search_args("to be modified")
    args = parser.parse_args_to_dict()

    app = PythonApp(VolumeModify, args)
    app.Run(**args)
