#!/usr/bin/env python

"""
This action will remove the user SSL certificate and return the cluster to the default
"""

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
def ClusterRemoveSslCert(mvip,
                     username,
                     password):
    """
    Remove the user SSL certificate and return the cluster to the default SSL certificate

    Args:
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    try:
        SFCluster(mvip, username, password).RemoveSSLCertificate()
    except SolidFireError as ex:
        log.error("Could not remove certificate on cluster: {}".format(ex))
        return False

    log.passed("Successfully removed user SSL certificate")
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterRemoveSslCert, args)
    app.Run(**args)
