#!/usr/bin/env python2.7

"""
This action will display the sfapp binary version on the nodes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfcluster import SFCluster
from libsf.sfnode import SFNode
from libsf.util import ValidateAndDefault, IPv4AddressType, StrType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def NodeGetBinaryVersion(mvip,
                         username,
                         password):
    """
    Get the sfapp binary version from the nodes

    Args:
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    # Get a list of nodes in the cluster
    log.info("Getting a list of nodes...")
    try:
        nodes = SFCluster(mvip, username, password).ListAllNodes()
    except SolidFireError as ex:
        log.error("Error getting list of nodes: {}".format(ex))
        return False

    flat_nodes = []
    for key in nodes.keys():
        flat_nodes.extend(nodes[key])
    node_ips = [node["mip"] for node in flat_nodes]

    # Launch a thread for each node
    log.info("Getting node versions...")
    pool = threadutil.GlobalPool()
    results = []
    for node_ip in node_ips:
        results.append(pool.Post(_NodeThread, node_ip, username, password))

    allgood = True
    for idx, node_ip in enumerate(node_ips):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: Error getting version info: {}".format(node_ip, e))
            allgood = False
            continue

    if not allgood:
        log.error("Could not get version from all nodes")
        return False

    return True


@threadutil.threadwrapper
def _NodeThread(node_ip, username, password):
    """Connect to the node and query the version info"""
    log = GetLogger()
    SetThreadLogPrefix(node_ip)

    node = SFNode(node_ip, clusterUsername=username, clusterPassword=password)
    ver = node.GetSfappVersion()

    log.info(" ".join(["{}={}".format(key, value) for key, value in ver.items()]))


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(NodeGetBinaryVersion, args)
    app.Run(**args)
