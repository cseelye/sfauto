#!/usr/bin/env python2.7

"""
This action will remove one or more active nodes from the cluster.
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster, DriveState
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, BoolType, StrType
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "node_ips" : (ItemList(IPv4AddressType), None),
    "by_node" : (BoolType, False),
    "remove_drives" : (BoolType, True),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ClusterRemoveNodes(node_ips,
                       by_node,
                       remove_drives,
                       mvip,
                       username,
                       password):
    """
    Remove nodes from the cluster

    Args:
        node_ips:       the MIPs of the active nodes to remove
        by_node:        remove the nodes one at a time instead of all at once
        remove_drives:  remove the drives from the nodes before removing the nodes
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)

    # remove_nodes is a list of lists that will be the order to remove nodes in
    # For each list in remove_nodes, we will remove all of those nodes at once and remove all of their drives at once
    remove_nodes = []
    if by_node:
        remove_nodes = [[node_ip] for node in node_ips]
    else:
        remove_nodes.append(node_ips)

    for node_list in remove_nodes:
        node_ids = cluster.GetNodeIDs(node_list)

        # Remove the drives
        if remove_drives:
            log.info("Removing drives from nodes [{}]".format(",".join(node_list)))
            drive_list = []
            for node_ip in node_list:
                try:
                    drive_list.extend(cluster.ListDrives(driveState=DriveState.Active, nodeIP=node_ip))
                except SolidFireError as ex:
                    log.error("Failed to get a list of drives in node {}".format(node_ip))
                    return False
            if drive_list:
                try:
                    cluster.RemoveDrives(drive_list, waitForSync=True)
                except SolidFireError as ex:
                    log.error("Failed to remove drives from [{}]".format(",".join(node_list)))
                    return False

        # Remove the nodes
        log.info("Removing nodes [{}]".format(",".join(node_list)))
        try:
            cluster.RemoveNodes(node_list)
        except SolidFireError as ex:
            log.error("Failed to remove nodes [{}]".format(",".join(node_list)))
            return False

    log.passed("Successfully removed {} from cluster".format(", ".join(node_ips)))
    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("-n", "--node_ips", type=ItemList(IPv4AddressType), required=True, help="the IP addresses of the nodes to remove")
    parser.add_argument("--bynode", action="store_true", dest="by_node", default=False, help="add the nodes one at a time instead of all at once")
    parser.add_argument("--nodrives", action="store_false",dest="remove_drives", default=True, help="do not remove the drives from the nodes before removing the nodes")
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterRemoveNodes, args)
    app.Run(**args)
