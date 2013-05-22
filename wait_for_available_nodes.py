#!/usr/bin/python

"""
This action will wait for a given list of pending nodes to be present

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --node_ips          The management IPs of the nodes to wait for

    --timeout           How long to wait (sec)
"""

import sys
from optparse import OptionParser
import time
import logging
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class WaitForAvailableNodesAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_WAIT = "BEFORE_WAIT"
        PENDING_LIST_CHANGED = "PENDING_LIST_CHANGED"
        ALL_NODES_FOUND = "ALL_NODES_FOUND"
        WAIT_TIMEOUT = "WAIT_TIMEOUT"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "nodeIPs" : libsf.IsValidIpv4AddressList,
                            "timeout" : libsf.IsInteger},
            args)
        if args["timeout"] < 0:
            raise libsf.SfArgumentError("timeout must be a positive integer")

    def Execute(self, nodeIPs, timeout=300, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Wait for the given list of pending nodes
        """
        if not nodeIPs:
            nodeIPs = []
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        nodeIPs = set(nodeIPs)

        mylog.info("Waiting up to " + str(timeout) + " sec for nodes " + ",".join(nodeIPs) + " to be available in cluster " + mvip)
        cluster = libsfcluster.SFCluster(mvip, username, password)
        start_time = time.time()
        self._RaiseEvent(self.Events.BEFORE_WAIT)
        previous_pending = set()
        while True:
            try:
                available = cluster.ListPendingNodes()
            except libsf.SfError as e:
                mylog.error(str(e))
                return False

            avail_ips = set()
            for node in available:
                avail_ips.add(node["mip"])

            # Display the list if it has changed
            if previous_pending & avail_ips != previous_pending:
                mylog.info("Pending node list: " + ",".join(avail_ips))
                self._RaiseEvent(self.Events.PENDING_LIST_CHANGED)
            previous_pending = avail_ips

            # Break if the list contains all of the requested nodes
            if nodeIPs & avail_ips == nodeIPs:
                self._RaiseEvent(self.Events.ALL_NODES_FOUND)
                return True

            time.sleep(10)
            if time.time() - start_time > timeout:
                mylog.error("Timeout waiting for pending nodes")
                self._RaiseEvent(self.Events.WAIT_TIMEOUT)
                return False

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
    parser.add_option("-n", "--node_ips", action="list", dest="node_ips", default=None, help="the management IPs of the nodes to wait for")
    parser.add_option("--timeout", type="int", dest="timeout", default=300, help="how long to wait (sec) before giving up [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ips, options.timeout, options.mvip, options.username, options.password, options.debug):
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

