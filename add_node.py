#!/usr/bin/python

"""
This action will add a node to the cluster.

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --node_ip           The managementIP of the node to add
"""

import sys
from optparse import OptionParser
import time
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class AddNodeAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_ADD = "BEFORE_ADD"
        AFTER_ADD = "AFTER_ADD"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "node_ip" : libsf.IsValidIpv4Address},
            args)

    def Execute(self, node_ip, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Add a node to the cluster
        """
        if not node_ip:
            node_ip = self.GetSharedValue("nodeIP")
        if not node_ip:
            node_ip = self.GetNextSharedValue("pendingNodeList")
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Find the nodeID of the requested node
        mylog.info("Searching for nodes")
        node_id = 0
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListPendingNodes", {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for node in result["pendingNodes"]:
            if node["mip"] == node_ip:
                node_id = node["pendingNodeID"]
                break
        if node_id <= 0:
            mylog.error("Could not find node " + node_ip)
            self.RaiseFailureEvent(failure="Could not find node " + node_ip)
            return False
        mylog.info("Found node " + node_ip + " is nodeID " + str(node_id))

        # Add the node
        mylog.info("Adding " + node_ip + " to cluster")
        self._RaiseEvent(self.Events.BEFORE_ADD)
        try:
            result = libsf.CallApiMethod(mvip, username, password, "AddNodes", {"pendingNodes" : [node_id]})
        except libsf.SfError as e:
            mylog.error("Failed to add node to cluster: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        time.sleep(20)

        mylog.passed("Successfully added " + node_ip + " to cluster")
        self._RaiseEvent(self.Events.AFTER_ADD)
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
    parser.add_option("--node_ip", type="string", dest="node_ip", default=None, help="the mIP of the node to add")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ip, options.mvip, options.username, options.password, options.debug):
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
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

