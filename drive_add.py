#!/usr/bin/env python

"""
This action will add available drives to the cluster from one or more nodes

When node_ips is specifed, it will add available drives from those nodes
When drive_slots is specified, it will add available drives from those slots
The two options can be combined to add avaialble drives that are in particular slots in particular nodes

When by_node is true, it will add the drives one node at a time instead of all at once

After drives are added it will wait for syncing to be complete
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster, DriveState
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, BoolType, StrType, OptionalValueType
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "node_ips" : (OptionalValueType(ItemList(IPv4AddressType)), None),
    "by_node" : (BoolType, False),
    "drive_slots" : (OptionalValueType(ItemList(int)), None),
    "wait_for_sync" : (BoolType, True),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def DriveAdd(node_ips,
             by_node,
             drive_slots,
             wait_for_sync,
             mvip,
             username,
             password):
    """
    Add available drives to the cluster, by node and/or slot, and wait for syncing

    Args:
        node_ips:       the MIPs of the nodes to add drives from
        by_node:        add the drives by node instead of all at once
        drive_slots:    the slots to add the drives from
        wait_for_sync:  wait for syncing to complete after adding the drives
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)

    log.info("Getting a list of nodes/drives")
    try:
        nodeip2nodeid = {node["mip"] : node["nodeID"] for node in cluster.ListActiveNodes()}
    except SolidFireError as e:
        log.error("Failed to list nodes: {}".format(e))
        return False
    if not node_ips:
        node_ips = list(nodeip2nodeid.keys())
    if not all({ip: ip in list(nodeip2nodeid.keys()) for ip in node_ips}.values()):
        log.error("Could not find all node IPs in cluster")
        return False

    node_ids = [nodeip2nodeid[node_ip] for node_ip in node_ips]

    try:
        drive_list = cluster.ListDrives()
    except SolidFireError as e:
        log.error("Failed to list drives: {}".format(e))
        return False

    # Make a list of drives to add
    # add_drives is a dictionary where the keys are the node IP and the value is the list of available drives from that node. 
    # Drives will then be added in the order of node_ips argument passed in.
    # Or if by_node is false, the key is "all" and the value is the list of all available drives
    add_drives = {}
    if by_node:
        for node_ip in node_ips:
            add_drives[node_ip] = []
            if drive_slots:
                add_drives[node_ip] = [drive for drive in drive_list if drive["status"] == DriveState.Available and \
                                                                        drive["nodeID"] == nodeip2nodeid[node_ip] and \
                                                                        drive["slot"] in drive_slots]
            else:
                add_drives[node_ip] = [drive for drive in drive_list if drive["status"] == DriveState.Available and \
                                                                        drive["nodeID"] == nodeip2nodeid[node_ip]]
    else:
        add_drives["all"] = []
        if not drive_slots:
            log.info("Getting drives from nodes [{}]".format(",".join(node_ips)))
            add_drives["all"] = [drive for drive in drive_list if drive["status"] == DriveState.Available and \
                                                                  drive["nodeID"] in node_ids]
        else:
            log.info("Getting drives in slots [{}] from nodes [{}]".format(",".join([str(s) for s in sorted(drive_slots)]), ",".join(node_ips)))
            add_drives["all"] = [drive for drive in drive_list if drive["status"] == DriveState.Available and \
                                                                  drive["nodeID"] in node_ids and \
                                                                  drive["slot"] in drive_slots]

    # Add the drives
    for node in add_drives.keys():
        count = len(add_drives[node])
        if node == "all" and count <= 0:
            log.info("No drives to add")
        elif node == "all":
            log.info("Adding {} drives".format(count))
        elif count <= 0:
            log.info("No drives to add from {}".format(node))
        else:
            log.info("Adding {} drives from {}".format(count, node))
        
        if count <= 0:
            continue

        try:
            cluster.AddDrives(add_drives[node], wait_for_sync)
        except SolidFireError as e:
            log.error("Failed to add drives: {}".format(e))
            return False

    log.passed("Finished adding drives")
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("-n", "--node_ips", type=ItemList(IPv4AddressType, allowEmpty=True), default=sfdefaults.node_ips, help="the IP addresses of the nodes to add drives from")
    parser.add_argument("--slots", dest="drive_slots", type=ItemList(int, allowEmpty=True), help="the slots to add the drives from")
    parser.add_argument("--bynode", action="store_true", dest="by_node", default=False, help="add the drives by node instead of all at once")
    parser.add_argument("--nosync", action="store_false",dest="wait_for_sync", default=True, help="do not wait for syncing after adding the drives")
    args = parser.parse_args_to_dict()

    app = PythonApp(DriveAdd, args)
    app.Run(**args)
