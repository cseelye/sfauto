#!/usr/bin/env python

"""
This action will display the cluster master node
"""
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, StrType, OptionalValueType, SelectionType
from libsf import sfdefaults
from libsf import SolidFireError
import sys

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
"output_format" : (OptionalValueType(SelectionType(sfdefaults.all_output_formats)), None),
})
def ClusterGetMasterNode(mvip,
                         username,
                         password,
                         output_format):
    """
    Display the cluster master node

    Args:
        output_format:      the output format to use; if specified logging will be silenced and the requested minimal format used
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)
    try:
        master = cluster.GetClusterMasterNode()
    except SolidFireError as ex:
        log.error(ex)
        return False
    
    # Display the list in the requested format
    if output_format == "bash":
        sys.stdout.write(master.ipAddress)
        sys.stdout.write("\n")
        sys.stdout.flush()
    elif output_format == "json":
        sys.stdout.write('{{"clusterMaster": "{}"}}'.format(master.ipAddress))
        sys.stdout.write("\n")
        sys.stdout.flush()
    else:
        log.info("Cluster master is {}".format(master.ipAddress))

    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_console_format_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClusterGetMasterNode, args)
    app.Run(**args)
