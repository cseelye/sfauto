#!/usr/bin/python

"""
This action will count the number of drives in a node

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --node_ips          IP address of the node

    --csv               Display minimal output that is suitable for piping into other programs

    --bash              Display minimal output that is formatted for a bash array/for loop
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class GetNodeDriveCountAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "node_ip" : libsf.IsValidIpv4Address},
            args)

    def Get(self, node_ip, mvip=sfdefaults.mvip, csv=False, bash=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Get the number of drives in a node
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

        # Find the nodeID of the requested node
        mylog.info("Searching for node")
        node_id = 0
        try:
            result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + e.message)
            return False
        for node in result["nodes"]:
            if node["mip"] == node_ip:
                node_id = node["nodeID"]
                break
        if node_id <= 0:
            mylog.error("Could not find node " + node_ip)
            return False

        # Count all of the drives (active, failed, vailable) in the node
        mylog.info("Searching for drives")
        drive_count = 0
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + e.message)
            return False
        for drive in result["drives"]:
            if drive["nodeID"] == node_id:
                drive_count += 1

       # self.SetSharedValue(SharedValues.nodeDriveCount, drive_count)
       # self.SetSharedValue(node_ip + "-driveCount", ipmi_ip)
        return drive_count

    def Execute(self, node_ip, mvip=sfdefaults.mvip, csv=False, bash=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Show the number of drives in a node
        """
        del self
        drive_count = Get(**locals())
        if drive_count is False:
            return False

        if csv or bash:
            sys.stdout.write(str(drive_count) + "\n")
            sys.stdout.flush()
        else:
            mylog.info("There are " + str(drive_count) + " drives in node " + node_ip)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--node_ip", type="string", dest="node_ip", default=None, help="the IP address of the node")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ip, options.mvip, options.csv, options.bash, options.username, options.password, options.debug):
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
        Abort()
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)

