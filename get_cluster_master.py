#!/usr/bin/env python2.7

"""
This action will display the cluster master

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

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

class GetClusterMasterAction(ActionBase):
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

    def Get(self, mvip=sfdefaults.mvip, csv=False, bash=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Get the cluster master
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

        # Node ID of the cluster master
        try:
            result = libsf.CallApiMethod(mvip, username, password, 'GetClusterMasterNodeID', {})
        except libsf.SfError as e:
            mylog.error("Failed to get cluster master ID - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        node_id = result["nodeID"]

        # Find the MIP of the cluster master
        try:
            result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        node_ip = None
        for node in result["nodes"]:
            if node["nodeID"] == node_id:
                node_ip = node["mip"]

        if not node_ip:
            mylog.error("Could not find cluster master IP")
            self.RaiseFailureEvent(message="Could not find cluster master IP")
            return False

        self.SetSharedValue(SharedValues.nodeIP, node_ip)
        self.SetSharedValue(SharedValues.clusterMasterIP, node_ip)
        self.SetSharedValue(SharedValues.clusterMasterID, node_id)
        return (node_ip, node_id)

    def Execute(self, mvip=sfdefaults.mvip, csv=False, bash=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Show the cluster master
        """
        del self
        result = Get(**locals())
        if result is False:
            return False
        (node_ip, node_id) = result

        if csv or bash:
            mylog.debug("Cluster " + mvip + " master node is " + node_ip + " (nodeID " + str(node_id) + ")")
            sys.stdout.write(node_ip + "\n")
            sys.stdout.flush()
        else:
            mylog.info("Cluster " + mvip + " master node is " + node_ip + " (nodeID " + str(node_id) + ")")
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
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.csv, options.bash, options.username, options.password, options.debug):
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

