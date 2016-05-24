#!/usr/bin/env python2.7

"""
This action will set the hostname on a node
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfnode import SFNode
from libsf.util import ValidateAndDefault, IPv4AddressType, StrType
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "node_ip" : (IPv4AddressType, None),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "hostname" : (StrType, None),
})
def NodeSetHostname(node_ip,
                    new_hostname,
                    username,
                    password):
    """
    Set the hostname on a node

    Args:
        node_ip:    the IP address of the node (str)
        username:   the cluster admin name (str)
        password:   the cluster admin password (str)
        hostname:   the hostname to set (str)
    """
    log = GetLogger()

    log.info("{}: Setting hostname to {}".format(node_ip, new_hostname))

    node = SFNode(ip=node_ip, clusterUsername=username, clusterPassword=password)
    try:
        node.SetHostname(new_hostname)
    except SolidFireError as e:
        log.error("Failed to set hostname: {}".format(e))
        return False

    log.passed("Successfully set hostname")
    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_single_node_args()
    parser.add_argument("--new-hostname", required=True, metavar="HOSTNAME", help="the hostname to set")
    args = parser.parse_args_to_dict()

    app = PythonApp(NodeSetHostname, args)
    app.Run(**args)
