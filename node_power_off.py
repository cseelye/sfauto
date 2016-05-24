#!/usr/bin/env python2.7

"""
This action will power off a node
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs, SetThreadLogPrefix
from libsf.sfnode import SFNode
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, OptionalValueType, StrType
from libsf import sfdefaults, threadutil, labutil
from libsf import SolidFireError, InvalidArgumentError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "node_ips" : (ItemList(IPv4AddressType), None),
    "ipmi_ips" : (OptionalValueType(ItemList(IPv4AddressType)), None),
    "ipmi_user" : (StrType, sfdefaults.ipmi_user),
    "ipmi_pass" : (StrType, sfdefaults.ipmi_pass),
})
def NodePowerOff(node_ips,
                 ipmi_ips,
                 ipmi_user,
                 ipmi_pass):
    """
    Power off nodes

    Args:
        node_ips:       the MIPs of the nodes to power off
        ipmi_ips:       the IPMI IPs of the nodes
        ipmi_user:      the IPMI username
        ipmi_pass:      the IPMI password
    """
    log = GetLogger()

    if ipmi_ips and len(node_ips) != len(ipmi_ips):
        raise InvalidArgumentError("If ipmi_ips is specified, it must have the same number of elements as node_ips")

    if ipmi_ips:
        node_ipmi = dict(zip(node_ips, ipmi_ips))
    else:
        log.info("Looking up IPMI info")
        node_ipmi = labutil.GetIPMIAddresses(node_ips)

    pool = threadutil.GlobalPool()
    results = []
    for node_ip in node_ips:
        results.append(pool.Post(_IPMIThread, node_ip, node_ipmi[node_ip], ipmi_user, ipmi_pass))

    allgood = True
    for idx, node_ip in enumerate(node_ips):
        try:
            
            results[idx].Get()
        except SolidFireError as e:
            log.error("  Error powering off node {}: {}".format(node_ip, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully powered of all nodes")
        return True
    else:
        log.error("Could not power off all nodes")
        return False

@threadutil.threadwrapper
def _IPMIThread(node_ip, ipmi_ip, ipmi_user, ipmi_pass):
    log = GetLogger()
    SetThreadLogPrefix(node_ip)
    
    log.info("Powering off")
    node = SFNode(node_ip, ipmiIP=ipmi_ip, ipmiUsername=ipmi_user, ipmiPassword=ipmi_pass)
    node.PowerOff()
    log.passed("Node is down")


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("-n", "--node_ips", type=ItemList(IPv4AddressType), required=True, help="the IP addresses of the nodes")
    parser.add_ipmi_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(NodePowerOff, args)
    app.Run(**args)
