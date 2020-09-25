#!/usr/bin/env python2.7

"""
This action will set an attribute on a list of volumes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateArgs, IPv4AddressType, OptionalValueType, ItemList, SolidFireIDType, PositiveIntegerType, BoolType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError, UnknownObjectError

@logargs
def VolumeSetAttribute(attribute_name,
                       attribute_value,
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
    Set an attribute on a list of volumes

    Args:
        attribute_name:     the name of the attribute
        attribute_value:    the value to set
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
    log = GetLogger()
    log.info("Starting set_volume_attributes.py")

    # Validate args
    allargs = ValidateArgs(locals(), {
        "attribute_name" : None,
        "attribute_value" : None,
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

    cluster = SFCluster(mvip, username, password)

    log.info("Searching for volumes")
    try:
        match_volumes = cluster.SearchForVolumes(volumeID=volume_ids, volumeName=volume_names, volumeRegex=volume_regex, volumePrefix=volume_prefix, accountName=source_account, accountID=source_account_id, volumeCount=volume_count)
    except UnknownObjectError:
        match_volumes = {}
    except SolidFireError as e:
        log.error("Failed to search for volumes: {}".format(e))
        return False

    if len(list(match_volumes.keys())) <= 0:
        log.warning("No matching volumes")
        return True

    log.info("{} volumes will be modified: {}".format(len(list(match_volumes.keys())), ",".join(sorted([vol["name"] for vol in match_volumes.values()]))))

    if test:
        log.warning("Test option set; volumes will not be modified")
        return True

    log.info("Modifying volumes...")
    pool = threadutil.GlobalPool()
    results = []
    for volume in match_volumes.values():
        log.info("  Setting attribute {} on volume {}".format(attribute_name, volume["name"]))
        attributes = volume["attributes"]
        attributes.update({attribute_name : attribute_value})
        results.append(pool.Post(_APICallThread, mvip, username, password, volume["volumeID"], attributes))

    allgood = True
    for idx, volume in enumerate(match_volumes.values()):
        try:
            
            results[idx].Get()
        except SolidFireError as e:
            log.error("  Error modifying volume {}: {}".format(volume["name"], e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully set attribute {} on all volumes".format(attribute_name))
        return True
    else:
        log.error("Could not set attribute {} on all volumes".format(attribute_name))
        return False

@threadutil.threadwrapper
def _APICallThread(mvip, username, password, volume_id, attributes):
    """Modify a volume, run as a thread"""

    # Make the change
    vol = SFCluster(mvip, username, password).ModifyVolume(volume_id, {"attributes" : attributes})

    # Verify that the change was applied
    if vol["attributes"] != attributes:
        raise SolidFireError("Attributes are not correct after modifying volume {} [expected={}, actual={}]".format(volume_id, attributes, vol["attributes"]))

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--attr", dest="attribute_name", required=True, metavar="NAME", help="The name of the attribute to set")
    parser.add_argument("--value", dest="attribute_value", required=True, metavar="VALUE", help="The value of the attribute")
    parser.add_volume_search_args("to be modified")
    args = parser.parse_args_to_dict()

    app = PythonApp(VolumeSetAttribute, args)
    app.Run(**args)
