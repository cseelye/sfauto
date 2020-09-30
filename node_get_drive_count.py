#!/usr/bin/env python

"""
This action will show the number of drives in a node
"""
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfnode import SFNode
from libsf.util import ValidateAndDefault, IPv4AddressType, SelectionType, OptionalValueType, StrType
from libsf import sfdefaults
from libsf import SolidFireError
import json
import sys

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "node_ip" : (IPv4AddressType, None),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "output_format" : (OptionalValueType(SelectionType(sfdefaults.all_output_formats)), None),
})
def NodeGetDriveCount(node_ip,
                      username,
                      password,
                      output_format):
    """
    Show the number of drives in a node

    Args:
        node_ip:        the management IP of the node
        username:       the admin user of the cluster
        password:       the admin password of the cluster
        output_format:  the output format to use; if specified logging will be silenced and the requested minimal format used
    """
    log = GetLogger()

    node = SFNode(ip=node_ip, clusterUsername=username, clusterPassword=password)
    try:
        config = node.GetDriveConfig()
    except SolidFireError as e:
        log.error("Failed to get drive config: {}".format(e))
        return False
    drive_count = config["numTotalActual"]

    # Display the list in the requested format
    if output_format and output_format == "bash":
        sys.stdout.write("{}\n".format(drive_count))
        sys.stdout.flush()
    elif output_format and output_format == "json":
        sys.stdout.write(json.dumps({"driveCount" : drive_count}) + "\n")
    else:
        log.info("There are {} drives in {}".format(drive_count, node_ip))

    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_single_node_args()
    parser.add_console_format_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(NodeGetDriveCount, args)
    app.Run(**args)
