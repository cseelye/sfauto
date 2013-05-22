#!/usr/bin/python

"""
This action will get the OS of a client

When run as a script, the following options/env variables apply:
    --client_ip        The IP address of the client

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class GetClusterInfoAction(ActionBase):
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
                            "password" : None},
            args)

    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Show cluster info
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Get Cluster version and volume count
        mylog.info( "========= Cluster Version =========")
        try:
            result = libsf.CallApiMethod(mvip, username, password, "GetClusterVersionInfo", {})
        except libsf.SfError as e:
            mylog.error("Failed to get cluster version: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        mylog.info( "ClusterVersion: " + str(result["clusterVersion"]))
        mylog.info("ClusterSize: " + str(len(result["clusterVersionInfo"])))
        nodeObj = result["clusterVersionInfo"]
        for node in nodeObj:
            mylog.info( "NodeID: " + str(node["nodeID"]) + " " + "Version: " + str(node["nodeInternalRevision"]))
        try:
            volResult = libsf.CallApiMethod(mvip, username, password, "ListActiveVolumes", {})
        except libsf.SfError as e:
            mylog.error("Failed to get volume list: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        mylog.info( "NumVolumes: " + str(len(volResult["volumes"])))

        # Get Cluster info
        mylog.info( "========= Cluster Info =========")
        try:
            clusterResult =  libsf.CallApiMethod(mvip, username, password, "GetClusterInfo", {})
        except libsf.SfError as e:
            mylog.error("Failed to get cluster info: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for key, value in clusterResult["clusterInfo"].iteritems():
            mylog.info( str(key) + " = " + str(value))

        # Get capacity info
        mylog.info( "========= Capacity Info =========")
        try:
            capacityResult = libsf.CallApiMethod(mvip, username, password, "GetClusterCapacity", {})
        except libsf.SfError as e:
            mylog.error("Failed to get cluster capacity: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for key, value in capacityResult["clusterCapacity"].iteritems():
            mylog.info( str(key) + " = " + str(value))

        # Get node info
        mylog.info( "========= Node Info =========")
        try:
            nodeResult = libsf.CallApiMethod(mvip, username, password, "ListAllNodes", {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        nodeObj = nodeResult["nodes"]
        for node in nodeObj:
            mylog.info(str(node["name"]) + " [" + str(node["nodeID"]) + "] " + str(node["mip"]))

        # Get SetConstants if needed
        mylog.info( "========= Constants =========")
        try:
            scResults =  libsf.CallApiMethod(mvip, username, password, "SetConstants", {})
        except libsf.SfError as e:
            mylog.error("Failed to get constsnts: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for key, value in scResults.iteritems():
            mylog.info( str(key) + " = " + str(value))

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
