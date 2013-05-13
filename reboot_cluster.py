#!/usr/bin/python

"""
This action will reboot all of the nodes in a cluster at the same time

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var
"""

import sys
from optparse import OptionParser
import time
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.libsfcluster as libsfcluster
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase

class RebootClusterAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_REBOOT = "BEFORE_REBOOT"
        AFTER_REBOOT = "AFTER_REBOOT"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None},
            args)

    def Execute(self, mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Reboot all cluster nodes at once
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        try:
            node_list = libsfcluster.SFCluster(mvip, username, password).ListActiveNodes()
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + str(e))
            super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, exception=e)
            return False

        params = {}
        params["nodes"] = []
        params["option"] = "restart"
        for node in node_list:
            params["nodes"].append(node["nodeID"])

        super(self.__class__, self)._RaiseEvent(self.Events.BEFORE_REBOOT)
        mylog.info("Rebooting " + str(len(node_list)) + " nodes in cluster " + str(mvip))
        try:
            libsf.CallApiMethod(mvip, username, password, 'Shutdown', params)
        except libsf.SfError as e:
            mylog.error("Failed to reboot nodes: " + str(e))
            super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, exception=e)
            return False

        for node in node_list:
            mylog.info("Waiting for " + node["mip"] + " to go down")
            while (libsf.Ping(node["mip"])):
                time.sleep(1)

        # Wait for the nodes to come back up
        for node in node_list:
            mylog.info("Waiting for " + node["mip"] + " to come up")
            while not libsf.Ping(node["mip"]):
                time.sleep(5)

        super(self.__class__, self)._RaiseEvent(self.Events.AFTER_REBOOT)
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
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
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
        Abort()
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)

