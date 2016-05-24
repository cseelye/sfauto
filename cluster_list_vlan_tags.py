#!/usr/bin/env python2.7

"""
This action will list the VLAN tags in a cluster
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, SelectionType, OptionalValueType, StrType
from libsf import sfdefaults
from libsf import SolidFireError
import sys
import json

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "output_format" : (OptionalValueType(SelectionType(sfdefaults.all_output_formats)), None),
})
def ClusterListVlanTags(mvip,
                        username,
                        password,
                        output_format):
    """
    Get the list of VLAN tags
    
    Args:
        output_format:      the output format to use; if specified logging will be silenced and the requested minimal format used
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    # Get the list of vlans
    log.info("Searching for VLANs")
    try:
        vlan_list = SFCluster(mvip, username, password).ListVLANs()
    except SolidFireError as e:
        log.error("Failed to list VLANs: {}".format(e))
        return False

    # Display the list in the requested format
    if output_format and output_format == "bash":
        sys.stdout.write(" ".join([vlan["virtualNetworkTag"] for vlan in vlan_list]) + "\n")
        sys.stdout.flush()
    elif output_format and output_format == "json":
        sys.stdout.write(json.dumps({"vlans" : [vlan["virtualNetworkTag"] for vlan in vlan_list]}) + "\n")
    else:
        log.info("{} VLANs in cluster {}".format(len(vlan_list), mvip))
        for vlan in vlan_list:
            log.info("  {}".format(vlan["virtualNetworkTag"]))

    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_console_format_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterListVlanTags, args)
    app.Run(**args)
