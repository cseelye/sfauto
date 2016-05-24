#!/usr/bin/env python2.7

"""
This action will set the time on a node
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
    "new_time" : (StrType, None),
})
def NodeSetTime(node_ip,
                new_time,
                username,
                password):
    """
    Set the time on a node

    Args:
        node_ip:    the IP address of the node (str)
        username:   the cluster admin name (str)
        password:   the cluster admin password (str)
        new_time:   the time to set, in a format 'date' will accept (str)
    """
    log = GetLogger()

    log.info("{}: Setting time to {}".format(node_ip, new_time))

    node = SFNode(ip=node_ip, clusterUsername=username, clusterPassword=password)
    try:
        node.SetTime(new_time)
    except SolidFireError as e:
        log.error("Failed to set time: {}".format(e))
        return False

    log.passed("Successfully set time")
    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_single_node_args()
    parser.add_argument("--new-time", required=True, metavar="TIME", help="the time to set, in a format 'date' will accept")
    args = parser.parse_args_to_dict()

    app = PythonApp(NodeSetTime, args)
    app.Run(**args)
