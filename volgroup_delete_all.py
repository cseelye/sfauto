#!/usr/bin/env python2.7

"""
This action will delete all volume access groups in a cluster
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateArgs, IPv4AddressType
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
def DeleteAllVolgroups(mvip=sfdefaults.mvip,
                       username=sfdefaults.username,
                       password=sfdefaults.password):
    """
    Delete all volume access groups
    """
    log = GetLogger()

    # Validate args
    ValidateArgs(locals(), {
        "mvip" : IPv4AddressType,
        "username" : None,
        "password" : None
    })

    cluster = SFCluster(mvip, username, password)

    log.info("Searching for volume groups")
    try:
        group_list = cluster.ListVolumeAccessGroups()
    except SolidFireError as e:
        log.error("Failed to list groups: {}".format(e))
        return False

    allgood = True
    for group in group_list:
        log.info("Deleting {}".format(group.name))
        try:
            group.Delete()
        except SolidFireError as e:
            log.error("Failed to delete group {}: {}".format(group.name, e))
            allgood = False

    if allgood:
        log.passed("Successfully deleted all groups")
        return True
    else:
        log.error("Failed to delete all groups")
        return False

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(DeleteAllVolgroups, args)
    app.Run(**args)
