#!/usr/bin/env python

"""
This action will get the active SSL certificate on the cluster
"""

from __future__ import print_function
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, StrType
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ClusterGetSslCert(mvip,
                     username,
                     password):
    """
    Get the active SSL certificate on the cluster

    Args:
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    try:
        certinfo = SFCluster(mvip, username, password).GetSSLCertificate()
    except SolidFireError as ex:
        log.error("Could not get certificate info on cluster: {}".format(ex))
        return False

    for key, value in certinfo.items():
        print("{}: {}".format(key, value))

    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterGetSslCert, args)
    app.Run(**args)
