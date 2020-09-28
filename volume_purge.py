#!/usr/bin/env python2.7

"""
This action will purge the deleted volumes on the cluster
"""
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.util import ValidateAndDefault, IPv4AddressType, StrType
from libsf.sfcluster import SFCluster
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def VolumePurge(mvip,
                username,
                password):
    """
    Purge the deleted volumes from the cluster

    Args:
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)
    log.info("Searching for volumes")
    try:
        volumes = cluster.ListDeletedVolumes()
    except SolidFireError as e:
        log.error("Failed to search for volumes: {}".format(e))
        return False

    volume_ids = [vol["volumeID"] for vol in volumes]

    if len(volume_ids) <= 0:
        log.passed("No deleted volumes to purge")
        return True

    log.info("Purging {} volumes...".format(len(volumes)))
    try:
        cluster.PurgeVolumes(volume_ids)
    except SolidFireError as e:
        log.error("Failed to purge volumes: {}".format(e))
        return False

    log.passed("Successfully purged {} volumes".format(len(volumes)))
    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(VolumePurge, args)
    app.Run(**args)
