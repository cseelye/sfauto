#!/usr/bin/env python2.7

"""
This action will power cycle a node
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs, SetThreadLogPrefix
from libsf.sfnode import SFNode
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, OptionalValueType, PositiveIntegerType, BoolType, StrType
from libsf import sfdefaults, threadutil, labutil
from libsf import SolidFireError, InvalidArgumentError
import time

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "node_ips" : (ItemList(IPv4AddressType), None),
    "ipmi_ips" : (OptionalValueType(ItemList(IPv4AddressType)), None),
    "wait_for_up" : (BoolType, True),
    "down_time" : (PositiveIntegerType, True),
    "ipmi_user" : (StrType, sfdefaults.ipmi_user),
    "ipmi_pass" : (StrType, sfdefaults.ipmi_pass),
})
def NodePowerCycle(node_ips,
                   ipmi_ips,
                   wait_for_up,
                   down_time,
                   ipmi_user,
                   ipmi_pass):
    """
    Power on nodes

    Args:
        node_ips:       the MIPs of the nodes to power on
        ipmi_ips:       the IPMI IPs of the nodes
        wait_for_up:    wait for the node to boot up before continuing
        down_time:      how long to wait between off and on (seconds)
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
        results.append(pool.Post(_IPMIThread, node_ip, node_ipmi[node_ip], ipmi_user, ipmi_pass, wait_for_up, down_time))

    allgood = True
    for idx, node_ip in enumerate(node_ips):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  Error powering on node {}: {}".format(node_ip, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully powered on all nodes")
        return True
    else:
        log.error("Could not power on all nodes")
        return False

@threadutil.threadwrapper
def _IPMIThread(node_ip, ipmi_ip, ipmi_user, ipmi_pass, wait_for_up, down_time):
    log = GetLogger()
    SetThreadLogPrefix(node_ip)

    node = SFNode(node_ip, ipmiIP=ipmi_ip, ipmiUsername=ipmi_user, ipmiPassword=ipmi_pass)

    log.info("Powering off")
    node.PowerOff()

    if down_time > 0:
        log.info("Waiting for {} seconds".format(down_time))
        time.sleep(sfdefaults.TIME_SECOND * down_time)

    log.info("Powering on")
    node.PowerOn(waitForUp=wait_for_up)
    if wait_for_up:
        log.passed("Node is up")
    else:
        log.passed("Node is on")


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("-n", "--node_ips", type=ItemList(IPv4AddressType), required=True, help="the IP addresses of the nodes")
    parser.add_argument("--down-time", type=PositiveIntegerType, default=0, help="how long to wait between off and on (seconds)")
    parser.add_argument("--nowait", action="store_false", dest="wait_for_up", default=True, help="do not wait for the node to come up")
    parser.add_ipmi_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(NodePowerCycle, args)
    app.Run(**args)
