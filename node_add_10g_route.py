#!/usr/bin/env python

"""
This action will add a network route to a node
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
    "node_ip" : (IPv4AddressType, None),
    "network" : (IPv4AddressType, None),
    "netmask" : (IPv4AddressType, None),
    "gateway" : (IPv4AddressType, None),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def NodeAdd10gRoute(node_ip,
                    network,
                    netmask,
                    gateway,
                    username,
                    password):
    """
    Add a network route to a node

    Args:
        node_ip:    the IP address of the node (string)
        username:   the cluster admin name (string)
        password:   the cluster admin password (string)
        network:    the network to route to (string)
        netmask:    the netmask for the route (string)
        gateway:    the gateway for the route (string)
    """
    log = GetLogger()

    log.info("{}: Adding route to {}/{} via {}".format(node_ip, network, netmask, gateway))

    node = SFNode(ip=node_ip, clusterUsername=username, clusterPassword=password)
    try:
        node.AddNetworkRoute10G(network, netmask, gateway)
    except SolidFireError as e:
        log.error("Failed to add route: {}".format(e))
        return False

    log.passed("Successfully added route")
    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_single_node_args()
    parser.add_argument("--network", type=str, dest="network", help="the network to route to")
    parser.add_argument("--netmask", type=str, dest="netmask", help="the subnet mask for the network")
    parser.add_argument("--gateway", type=str, dest="gateway", help="the gateway to the network")
    args = parser.parse_args_to_dict()

    app = PythonApp(NodeAdd10gRoute, args)
    app.Run(**args)
