#!/usr/bin/env python

"""
This action will empty the log files on a cluster
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs, SetThreadLogPrefix
from libsf.sfcluster import SFCluster
from libsf.sfnode import SFNode
from libsf.util import ValidateAndDefault, IPv4AddressType, StrType
from libsf import sfdefaults, threadutil
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "ssh_user" : (StrType, sfdefaults.ssh_user),
    "ssh_pass" : (StrType, sfdefaults.ssh_pass),
})
def ClusterCleanLogs(mvip,
                     username,
                     password,
                     ssh_user,
                     ssh_pass):
    """
    Clean the logs

    Args:
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
        ssh_user:
        ssh_pass:
    """
    log = GetLogger()

    log.info("Searching for nodes")
    try:
        node_list = SFCluster(mvip, username, password).ListAllNodes()
    except SolidFireError as e:
        log.error("Could not search for nodes: {}".format(e))
        return False
    node_ips = [node["mip"] for node in node_list]

    pool = threadutil.GlobalPool()
    results = []
    for node_ip in node_ips:
        results.append(pool.Post(_NodeThread, node_ip, ssh_user, ssh_pass))

    allgood = True
    for idx, node_ip in enumerate(node_ips):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  Error cleaning logs on node {}: {}".format(node_ip, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully cleaned logs on all nodes")
        return True
    else:
        log.error("Could not clean logs on all nodes")
        return False


@threadutil.threadwrapper
def _NodeThread(node_ip, ssh_user, ssh_pass):
    log = GetLogger()
    SetThreadLogPrefix(node_ip)

    log.info("Rotating logs")
    node = SFNode(node_ip, sshUsername=ssh_user, sshPassword=ssh_pass)
    node.CleanLogs()

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_node_ssh_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterCleanLogs, args)
    app.Run(**args)
