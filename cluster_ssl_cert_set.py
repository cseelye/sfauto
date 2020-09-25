#!/usr/bin/env python2.7

"""
This action will set the SSL certificate for a cluster
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, StrType
from libsf import sfdefaults
from libsf import SolidFireError
from io import open

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "cert" : (StrType, None),
    "key" : (StrType, None),
})
def ClusterSetSslCert(mvip,
                     username,
                     password,
                     cert,
                     key):
    """
    Set the SSL certificate used by the cluster

    Args:
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
        cert:           the PEM encoded certificate file
        key:            the PEM encoded private key file
    """
    log = GetLogger()

    try:
        with open(cert, "r") as infile:
            cert_contents = infile.read()
    except IOError as ex:
        log.error("Could not read certificate file: {}".format(ex))
        return False

    try:
        with open(key, "r") as infile:
            key_contents = infile.read()
    except IOError as ex:
        log.error("Could not read key file: {}".format(ex))
        return False

    try:
        SFCluster(mvip, username, password).SetSSLCertificate(cert_contents, key_contents)
    except SolidFireError as ex:
        log.error("Could not set certificate on cluster: {}".format(ex))
        return False

    log.passed("Successfully set SSL certificate")
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--cert", type=StrType, required=True, help="the PEM encoded certificate file")
    parser.add_argument("--key", type=StrType, required=True, help="the PEM encoded private key file")
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterSetSslCert, args)
    app.Run(**args)
