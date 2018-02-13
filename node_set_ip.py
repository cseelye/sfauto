#!/usr/bin/env python2.7

"""
This action will set the network config on a node
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfnode import SFNode
from libsf.util import ValidateAndDefault, IPv4AddressType, IPv4SubnetType, StrType, OptionalValueType
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "node_ip" : (IPv4AddressType, None),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "management_ip" : (OptionalValueType(IPv4AddressType), None),
    "management_netmask" : (OptionalValueType(IPv4SubnetType), None),
    "management_gateway" : (OptionalValueType(IPv4AddressType), None),
    "dns_ip" : (OptionalValueType(IPv4AddressType), None),
    "dns_search" : (OptionalValueType(StrType), None),
    "storage_ip" : (OptionalValueType(IPv4AddressType), None),
    "storage_netmask" : (OptionalValueType(IPv4SubnetType), None),
    "storage_gateway" : (OptionalValueType(IPv4AddressType), None),
})
def NodeSetIp(node_ip,
              management_ip,
              management_netmask,
              management_gateway,
              dns_ip,
              dns_search,
              storage_ip,
              storage_netmask,
              storage_gateway,
              username,
              password):
    """
    Set the network config on a node

    Args:
        node_ip:                the current IP address of the node (str)
        username:               the cluster admin name (str)
        password:               the cluster admin password (str)
        management_ip:          the IP address for the managment NIC (MIPI)
        management_netmask:     the netmask for the managment NIC (MIPI)
        management_gateway:     the gateway for the managment NIC (MIPI)
        dns_ip:                 the IP of the DNS server
        dns_search:             the search string for DNS lookups
        storage_ip:             the IP address for the storage NIC (SIPI)
        storage_netmask:        the netmask for the storage NIC (SIPI)
        storage_gateway:        the gateway for the storage NIC (SIPI)
    """
    log = GetLogger()

    log.info("Updating network configuration")
    node = SFNode(ip=node_ip, clusterUsername=username, clusterPassword=password)
    try:
        node.SetNetworkConfig(managementIP=management_ip,
                              managementNetmask=management_netmask, 
                              managementGateway=management_gateway,
                              dnsIP=dns_ip,
                              dnsSearch=dns_search,
                              storageIP=storage_ip,
                              storageNetmask=storage_netmask,
                              storageGateway=storage_gateway)
    except SolidFireError as e:
        log.error("Failed to set network config: {}".format(e))
        return False

    log.passed("Successfully set network config")
    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_single_node_args()
    parser.add_argument("--management-ip", required=False, metavar="IP", help="the new management IP to set")
    parser.add_argument("--management-netmask", required=False, metavar="NETMASK", help="the new management subnet mask to set")
    parser.add_argument("--management-gateway", required=False, metavar="IP", help="the new management default gateway to set")
    parser.add_argument("--dns-ip", required=False, metavar="IP", help="the new DNS server IP to set")
    parser.add_argument("--dns-search", required=False, metavar="IP", help="the new DNS search domain to set")
    parser.add_argument("--storage-ip", required=False, metavar="IP", help="the new storage IP to set")
    parser.add_argument("--storage-netmask", required=False, metavar="IP", help="the new storage subnet mask to set")
    parser.add_argument("--storage-gateway", required=False, metavar="IP", help="the new storage default gateway to set")
    args = parser.parse_args_to_dict()

    app = PythonApp(NodeSetIp, args)
    app.Run(**args)
