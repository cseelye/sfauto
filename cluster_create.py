#!/usr/bin/env python2.7

"""
This action will create a cluster
"""
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, PositiveNonZeroIntegerType, OptionalValueType, PositiveIntegerType, BoolType, StrType
from libsf import sfdefaults
from libsf import SolidFireError, SolidFireBootstrapAPI
import time

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "node_ip" : (IPv4AddressType, None),
    "svip" : (IPv4AddressType, sfdefaults.svip),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "node_count" : (OptionalValueType(PositiveNonZeroIntegerType), None),
    "node_timeout" : (PositiveIntegerType, 60),
    "drive_count" : (OptionalValueType(PositiveNonZeroIntegerType), None),
    "drive_timeout" : (PositiveIntegerType, 180),
    "add_drives" : (BoolType, True),
})
def ClusterCreate(node_ip,
                  svip,
                  mvip,
                  username,
                  password,
                  node_count,
                  node_timeout,
                  drive_count,
                  drive_timeout,
                  add_drives):
    """
    Create a cluster

    Args:
        node_ip:            the IP of the node to use to create the cluster
        svip:               the storage IP of the cluster
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
        node_count:         wait for this many nodes before creating the cluster
        node_timeout:       how long to wait for available nodes, in seconds
        drive_count:        after creating the cluster, wait for this many drives to be available
        drive_timeout:      how long to wait for available drives, in seconds
        add_drives:         After creating the cluster, add the available drives
    """
    log = GetLogger()

    bootstrap = SolidFireBootstrapAPI(node_ip)

    # Wait for the correct number of nodes to be present
    if node_count:
        log.info("Waiting for {} available nodes...".format(node_count))
        previous_nodes = ""
        start_time = time.time()
        while True:
            try:
                nodes = bootstrap.GetBootstrapNodes()
            except SolidFireError as ex:
                log.error("Failed to get bootstrap config: {}".format(ex))
                return False

            display_nodes = ",".join(sorted(nodes))
            if display_nodes != previous_nodes:
                log.info("Found {} nodes [{}]".format(len(nodes), display_nodes))
                previous_nodes = display_nodes

            if len(nodes) >= node_count:
                break
            time.sleep(1 * sfdefaults.TIME_SECOND)

            if time.time() - start_time > node_timeout:
                log.error("Timeout waiting for nodes")
                return False

    # Create the cluster
    log.info("Creating cluster")
    try:
        bootstrap.CreateCluster(mvip, svip, username, password)
    except SolidFireError as ex:
        log.error("Failed to create cluster: {}".format(ex))
        return False

    if not add_drives:
        log.passed("Successfully created cluster")
        return True

    cluster = SFCluster(mvip, username, password)

    # Determine how many drives the cluster should expect
    if not drive_count:
        try:
            nodes = cluster.GetActiveNodeObjects()
        except SolidFireError as ex:
            log.error("Failed to list nodes: {}".format(ex))

        # Get the expected rive count form each node
        drive_count = 0
        for node in nodes:
            try:
                drive_count += node.GetExpectedDriveCount()
            except SolidFireError as ex:
                log.error("  {}: Failed to get drive count: {}".format(node.ipAddress, ex))
                return False

    # Wait for available drives
    start_time = time.time()
    previous_count = -1
    log.info("Waiting for {} available drives...".format(drive_count))
    while True:
        try:
            drives = cluster.ListAvailableDrives()
        except SolidFireError as ex:
            log.error("Failed to list drives: {}".format(ex))
            return False

        if len(drives) != previous_count:
            log.info("Found {} available drives".format(len(drives)))
            previous_count = len(drives)

        if len(drives) >= drive_count:
            break

        if time.time() - start_time > drive_timeout:
            log.error("Timeout waiting for available drives")
            return False

    # Add the available drives
    log.info("Adding available drives")
    try:
        cluster.AddAvailableDrives(waitForSync=False)
    except SolidFireError as ex:
        log.error("Failed to add drives: {}".format(ex))
        return False

    log.passed("Successfully created cluster")
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("-n", "--node-ip", type=IPv4AddressType, required=True, metavar="IP", help="the management IP of the node to create the cluster with")
    parser.add_argument("-m", "--mvip", type=IPv4AddressType, required=True, default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_argument("--svip", type=IPv4AddressType, required=True, default=sfdefaults.svip, help="the storage IP of the cluster")
    parser.add_argument("-u", "--username", type=str, required=True, default=sfdefaults.username, help="the admin user for the cluster")
    parser.add_argument("-p", "--password", type=str, required=True, default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_argument("--node-count", type=PositiveIntegerType, default=None, metavar="COUNT", help="how many available nodes to wait for")
    parser.add_argument("--node-timeout", type=PositiveNonZeroIntegerType, default=60, metavar="SECONDS", help="how long to wait for available nodes, in seconds")
    parser.add_argument("--drive-count", type=PositiveIntegerType, default=None, metavar="COUNT", help="how many available drives to wait for")
    parser.add_argument("--drive-timeout", type=PositiveNonZeroIntegerType, default=180, metavar="SECONDS", help="how long to wait for available drives, in seconds")
    parser.add_argument("--no-add-drives", dest="add_drives", action="store_false", default=True, help="do not add the drives to the cluster")
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterCreate, args)
    app.Run(**args)
