#!/usr/bin/env python2.7

"""
This action will create a VLAN on a cluster
"""
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, PositiveNonZeroIntegerType, BoolType, StrType, VLANTagType
from libsf import sfdefaults
from libsf import SolidFireError, SolidFireAPIError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "tag" : (VLANTagType, None),
    "address_start" : (IPv4AddressType, None),
    "address_count" : (PositiveNonZeroIntegerType, None),
    "netmask" : (IPv4AddressType, None),
    "svip" : (IPv4AddressType, None),
    "namespace" : (BoolType, False),
    "strict" : (BoolType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def CreateVlan(tag,
               address_start,
               address_count,
               netmask,
               svip,
               namespace,
               strict,
               mvip,
               username,
               password):
    """
    Start GC on the cluster

    Args:
        tag:                the tag for the VLAN
        address_start:      the starting address for the nodes on the VLAN
        address_count:      the number of addresses for nodes on the VLAN
        netmask:            the netmask for the nodes on the VLAN
        svip:               the SVIP for the VLAN
        namespace:          put this VLAN in a namespace
        strict:             fail if the VLAN already exists
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)
    try:
        cluster.CreateVLAN(tag, address_start, address_count, netmask, svip, namespace)
    except SolidFireAPIError as ex:
        if ex.code == "xVirtualNetworkAlreadyExists" and not strict:
            log.passed("VLAN already exists")
            return True
        else:
            log.error("VLAN already exists")
            return False
    except SolidFireError as ex:
        log.error(str(ex))
        return False

    log.passed("Successfully created VLAN")
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--tag", type=VLANTagType, required=True, help="the number of addresses for nodes on the VLAN")
    parser.add_argument("--address-start", type=IPv4AddressType, required=True, help="the starting address for nodes on the VLAN")
    parser.add_argument("--address-count", type=PositiveNonZeroIntegerType, required=True, help="the number of addresses for nodes on the VLAN")
    parser.add_argument("--netmask", type=str, required=True, help="the subnet mask for the nodes on the VLAN")
    parser.add_argument("--svip", type=IPv4AddressType, required=True, default=sfdefaults.svip, help="the storage IP for the VLAN")
    parser.add_argument("--namespace", action="store_true", default=False, help="put this VLAN into a namespace")
    parser.add_argument("--strict", action="store_true", default=False, help="fail if the VLAN already exists")
    parser.add_cluster_mvip_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(CreateVlan, args)
    app.Run(**args)
