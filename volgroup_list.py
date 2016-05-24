#!/usr/bin/env python2.7

"""
This action will list the volume access groups in a cluster
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateArgs, IPv4AddressType, SelectionType, OptionalValueType
from libsf import sfdefaults
from libsf import SolidFireError
import sys
import json

@logargs
def ListVolgroups(mvip=sfdefaults.mvip,
                    username=sfdefaults.username,
                    password=sfdefaults.password,
                    output_format=None):
    """
    Get the list of volume groups
    
    Args:
        format:     the output format to use; if specified logging will be silenced and the requested minimal format used
        mvip:       the management IP of the cluster
        username:   the admin user of the cluster
        password:   the admin password of the cluster
    """
    log = GetLogger()

    # Validate args
    ValidateArgs(locals(), {
        "output_format" : OptionalValueType(SelectionType(sfdefaults.all_output_formats)),
        "mvip" : IPv4AddressType,
        "username" : None,
        "password" : None
    })

    # Get the list of groups
    log.info("Searching for volume groups")
    try:
        group_list = SFCluster(mvip, username, password).ListVolumeAccessGroups()
    except SolidFireError as e:
        log.error("Failed to list groups: {}".format(e))
        return False

    # Display the list in the requested format
    if output_format and output_format == "bash":
        sys.stdout.write(" ".join([group.name for group in group_list]) + "\n")
        sys.stdout.flush()
    elif output_format and output_format == "json":
        sys.stdout.write(json.dumps({"volumeAccessGroups" : [group.name for group in group_list]}) + "\n")
    else:
        log.info("{} volume groups in cluster {}".format(len(group_list), mvip))
        for group in group_list:
            log.info("  {}".format(group.name))

    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_console_format_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ListVolgroups, args)
    app.Run(**args)
