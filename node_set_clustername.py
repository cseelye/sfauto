#!/usr/bin/env python2.7

"""
This action will set the cluster name on a list of nodes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs, SetThreadLogPrefix
from libsf.sfnode import SFNode
from libsf.util import ValidateAndDefault, IPv4AddressType, ItemList, StrType
from libsf import threadutil
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "node_ips" : (ItemList(IPv4AddressType), None),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "cluster_name" : (StrType, None),
})
def NodeSetClustername(node_ips,
                          cluster_name,
                          username,
                          password):
    """
    Set the cluster name on a list of nodes

    Args:
        node_ips:           the IP address of the nodes (string)
        username:           the node admin name (string)
        password:           the node admin password (string)
        cluster_name        the cluster name to set
    """
    log = GetLogger()

    pool = threadutil.GlobalPool()
    results = []
    for idx, node_ip in enumerate(node_ips):
        results.append(pool.Post(_NodeThread, cluster_name, node_ip, username, password))

    allgood = True
    for idx, node_ip in enumerate(node_ips):
        try:
            ret = results[idx].Get()
        except SolidFireError as e:
            log.error("  Error setting cluster name on node {}: {}".format(node_ip, e))
            allgood = False
            continue
        if not ret:
            allgood = False

    if allgood:
        log.passed("Successfully set cluster name on all nodes")
        return True
    else:
        log.error("Could not set cluster name on all nodes")
        return False

@threadutil.threadwrapper
def _NodeThread(cluster_name, node_ip, username, password):
    """Set cluster name on a node"""
    log = GetLogger()
    SetThreadLogPrefix(node_ip)

    node = SFNode(node_ip, clusterUsername=username, clusterPassword=password)
    log.info("Setting cluster name to {}".format(cluster_name))
    node.SetClusterName(cluster_name)

    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_node_list_args()
    parser.add_argument("--cluster-name", required=True, metavar="NAME", help="the name of the cluster")
    args = parser.parse_args_to_dict()

    app = PythonApp(NodeSetClustername, args)
    app.Run(**args)
