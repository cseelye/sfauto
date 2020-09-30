#!/usr/bin/env python

"""
This action will display a list of the nodes in the cluster
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, SelectionType, OptionalValueType, BoolType, StrType
from libsf import sfdefaults
from libsf import SolidFireError
import sys
import json

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "node_state" : (SelectionType(sfdefaults.all_node_states), "all"),
    "by_id" : (BoolType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "output_format" : (OptionalValueType(SelectionType(sfdefaults.all_output_formats)), None),
})
def ClusterListNodes(node_state,
                     by_id,
                     mvip,
                     username,
                     password,
                     output_format):
    """
    Get the list of nodes

    Args:
        node_state:     display nodes in this state (pending, active, all)
        by_id:          show node IDs instead of mIP addresses
        output_format:  the output format to use; if specified logging will be silenced and the requested minimal format used
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    log.info("Searching for nodes")
    try:
        node_list = SFCluster(mvip, username, password).ListAllNodes()
    except SolidFireError as e:
        log.error("Could not search for nodes: {}".format(e))
        return False

    flat_list = []
    if node_state == "all":
        for n in node_list.values():
            flat_list.extend(n)
    elif node_state == "active":
        flat_list = node_list["nodes"]
    elif node_state == "pending":
        flat_list = node_list["pendingNodes"]

    attr = "mip"
    if by_id:
        attr = "nodeID"
    nodes = [node[attr] for node in flat_list]

    # Display the list in the requested format
    if output_format and output_format == "bash":
        sys.stdout.write(" ".join([str(item) for item in nodes]) + "\n")
        sys.stdout.flush()
    elif output_format and output_format == "json":
        sys.stdout.write(json.dumps({"nodes" : nodes}) + "\n")
        sys.stdout.flush()
    else:
        if node_state == "all":
            log.info("{} nodes in cluster".format(len(nodes)))
        else:
            log.info("{} {} nodes in cluster".format(len(nodes), node_state))

        if len(nodes) > 0:
            log.info("  {}".format(", ".join([str(item) for item in nodes])))

    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--byid", action="store_true", default=False, dest="by_id", help="display volume IDs instead of volume names")
    parser.add_argument("--state", dest="node_state", choices=sfdefaults.all_node_states, default="all", help="display nodes in this state")
    parser.add_console_format_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterListNodes, args)
    app.Run(**args)
