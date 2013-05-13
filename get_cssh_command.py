#!/usr/bin/python

"""
This script will display the cluster ssh command for the nodes in a cluster
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults

def ValidateArgs(args):
    libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                        "username" : None,
                        "password" : None},
        args)

def Execute(mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
    """
    Get a list of active nodes in the cluster
    """

    ValidateArgs(locals())
    if debug:
        mylog.console.setLevel(logging.DEBUG)

    # Get a list of nodes in the cluster
    try:
        node_list = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
    except libsf.SfError as e:
        mylog.error("Failed to get node list: " + e.message)
        return False

    node_mips = []
    for node in node_list["nodes"]:
        node_mips.append(node["mip"])

    mylog.raw(" cssh -l root " + " ".join(node_mips))

    return True


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        if Execute(options.mvip, options.username, options.password, options.debug):
            sys.exit(0)
        else:
            sys.exit(1)
    except libsf.SfArgumentError as e:
        mylog.error("Invalid arguments - \n" + str(e))
        sys.exit(1)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

