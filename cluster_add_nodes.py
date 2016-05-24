#!/usr/bin/env python2.7

"""
This action will add one or more pending nodes to the cluster.
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, BoolType, StrType
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "node_ips" : (ItemList(IPv4AddressType), None),
    "rtfi" : (BoolType, True),
    "by_node" : (BoolType, False),
    "add_drives" : (BoolType, True),
    "wait_for_sync" : (BoolType, True),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ClusterAddNodes(node_ips,
                    rtfi,
                    by_node,
                    add_drives,
                    wait_for_sync,
                    mvip,
                    username,
                    password):
    """
    Add nodes to the cluster

    Args:
        node_ips:       the MIPs of the pending nodes to add
        rtfi:           autoRTFI the nodes when adding them
        by_node:        add the nodes one at a time instead of all at once
        add_drives:     add the drives from the nodes after adding the nodes
        wait_for_sync:  wait for syncing to complete after adding the drives
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)

    # Add nodes is a list of lists that will be the order to add nodes in
    # For each list in add_nodes, we will add all of those nodes at once and add all of their drives at once
    add_nodes = []
    if by_node:
        for node_ip in node_ips:
            add_nodes.append([node_ip])
    else:
        add_nodes.append(node_ips)

    for node_list in add_nodes:
        # Add the nodes to the cluster
        log.info("Adding [{}] to cluster".format(",".join(node_list)))
        try:
            cluster.AddNodes(node_list, autoRTFI=rtfi)
        except SolidFireError as e:
            log.error("Failed to add {}: {}".format(",".join(node_list), e))
            return False

        if add_drives:
            # Wait for the drives in all the added nodes to be available
            drive_list = []
            for node_ip in node_list:
                log.info("Waiting for available drives in {}".format(node_ip))
                try:
                    drive_list += cluster.WaitForAvailableDrives(nodeIP=node_ip)
                except SolidFireError as e:
                    log.error("Failed waiting for available drives in {}: {}".format(node_ip, e))
                    return False

            # Add the drives from the nodes to the cluster
            log.info("Adding {} drives to cluster".format(len(drive_list)))
            try:
                cluster.AddDrives(drive_list, waitForSync=wait_for_sync)
            except SolidFireError as e:
                log.error("Failed to add drives from {}: {}".format(",".join(node_list), e))
                return False

    log.passed("Successfully added {} to cluster".format(", ".join(node_ips)))
    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("-n", "--node_ips", type=ItemList(IPv4AddressType), required=True, default=sfdefaults.node_ips, help="the IP addresses of the nodes to add")
    parser.add_argument("--bynode", action="store_true", default=False, help="add the nodes one at a time instead of all at once")
    parser.add_argument("--nortfi", action="store_false",dest="rtfi", default=True, help="do not auto RTFI the node when adding")
    parser.add_argument("--nodrives", action="store_false",dest="add_drives", default=True, help="do not add the drives from the nodes after adding the nodes")
    parser.add_argument("--nosync", action="store_false",dest="wait_for_sync", default=True, help="do not wait for syncing after adding the drives")
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterAddNodes, args)
    app.Run(**args)
