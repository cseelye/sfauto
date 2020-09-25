#!/usr/bin/env python2.7

"""
This action will remove active drives from the cluster

When node_ips is specified, it will remove active drives only from those nodes
When drive_slots is specified, it will remove active drives from only those slots
When by_node is true, it will remove the active drives one node at a time instead of all at once

After drives are removed it will wait for syncing to be complete
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
    "node_ips" : (ItemList(IPv4AddressType), None),
    "by_node" : (BoolType, False),
    "drive_slots" : (OptionalValueType(ItemList(int)), None),
    "wait_for_sync" : (BoolType, True),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def DriveRemove(node_ips,
                by_node,
                drive_slots,
                wait_for_sync,
                mvip,
                username,
                password):
    """
    Remove drives from the cluster, by node and/or slot, and wait for syncing

    Args:
        node_ips:       the MIPs of the nodes to remove drives from
        by_node:        remove the drives by node instead of all at once
        drive_slots:    the slots to remove the drives from
        wait_for_sync:  wait for syncing to complete after removing the drives
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
    if not all({ip: ip in list(nodeip2nodeid.keys()) for ip in node_ips}.values()):
        log.error("Could not find all node IPs in cluster")
        return False

    node_ids = [nodeip2nodeid[node_ip] for node_ip in node_ips]

    try:
        drive_list = cluster.ListDrives()
    except SolidFireError as e:
        log.error("Failed to list drives: {}".format(e))
        return False

    # Make a list of drives to remove
    # rem_drives is a dictionary where the keys are the node IP and the value is the list of drives to remove from that node. 
    # Drives will then be removed in the order of node_ips argument passed in.
    # Or if by_node is false, the key is "all" and the value is the list of all drives to remove
    rem_drives = {}
    if by_node:
        for node_ip in node_ips:
            rem_drives[node_ip] = []
            if drive_slots:
                rem_drives[node_ip] = [drive for drive in drive_list if drive["status"] == DriveState.Active and \
                                                                        drive["nodeID"] == nodeip2nodeid[node_ip] and \
                                                                        drive["slot"] in drive_slots]
            else:
                rem_drives[node_ip] = [drive for drive in drive_list if drive["status"] == DriveState.Active and \
                                                                        drive["nodeID"] == nodeip2nodeid[node_ip]]
    else:
        rem_drives["all"] = []
        if not drive_slots:
            log.info("Getting drives from nodes [{}]".format(",".join(node_ips)))
            rem_drives["all"] = [drive for drive in drive_list if drive["status"] == DriveState.Active and \
                                                                  drive["nodeID"] in node_ids]
        else:
            log.info("Getting drives in slots [{}] from nodes [{}]".format(",".join([str(s) for s in sorted(drive_slots)]), ",".join(node_ips)))
            rem_drives["all"] = [drive for drive in drive_list if drive["status"] == DriveState.Active and \
                                                                  drive["nodeID"] in node_ids and \
                                                                  drive["slot"] in drive_slots]

    # Add the drives
    for node in rem_drives.keys():
        count = len(rem_drives[node])
        if node == "all" and count <= 0:
            log.info("No drives to remove")
        elif node == "all":
            log.info("Removing {} drives".format(count))
        elif count <= 0:
            log.info("No drives to remove from {}".format(node))
        else:
            log.info("Removing {} drives from {}".format(count, node))
        
        if count <= 0:
            continue

        try:
            cluster.RemoveDrives(rem_drives[node], wait_for_sync)
        except SolidFireError as e:
            log.error("Failed to remove drives: {}".format(e))
            return False

    log.passed("Finished removing drives")
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("-n", "--node_ips", type=ItemList(IPv4AddressType), required=True, default=sfdefaults.node_ips, help="the IP addresses of the nodes to remove drives from")
    parser.add_argument("--slots", dest="drive_slots", type=ItemList(int, allowEmpty=True), help="the slots to remove the drives from")
    parser.add_argument("--bynode", action="store_true", dest="by_node", default=False, help="remove the drives by node instead of all at once")
    parser.add_argument("--nosync", action="store_false",dest="wait_for_sync", default=True, help="do not wait for syncing after removing the drives")
    args = parser.parse_args_to_dict()

    app = PythonApp(DriveRemove, args)
    app.Run(**args)
