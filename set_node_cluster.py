#!/usr/bin/python

"""
This action will set the cluster name of an available node

When run as a script, the following options/env variables apply:
    --node_ip           The node management IP

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --cluster_name      The name of the cluster
"""

import sys
from optparse import OptionParser
import logging
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import lib.libsfnode as libsfnode

class SetNodeClusterAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_SET_CLUSTER_NAME = "BEFORE_SET_CLUSTER_NAME"
        AFTER_SET_CLUSTER_NAME = "AFTER_SET_CLUSTER_NAME"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"nodeIPs" : libsf.IsValidIpv4AddressList,
                            "username" : None,
                            "password" : None,
                            "clusterName": None},
            args)

    def Execute(self, nodeIPs, clusterName, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Set the cluster name of an available node
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        allgood = True
        for node_ip in nodeIPs:
            self._RaiseEvent(self.Events.BEFORE_SET_CLUSTER_NAME, clusterName=clusterName, nodeIP=node_ip)
            mylog.info("Setting cluster to '" + clusterName + "' on node " + str(node_ip))

            node = libsfnode.SFNode(ip=node_ip, clusterUsername=username, clusterPassword=password)
            try:
                node.SetClusterName(clusterName)
            except libsf.SfError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), nodeIP=node_ip, clusterName=clusterName, exception=e)
                allgood = False
                continue

            mylog.passed("Successfully set cluster name")
            self._RaiseEvent(self.Events.AFTER_SET_CLUSTER_NAME, nodeIP=node_ip, clusterName=clusterName)

        if allgood:
            mylog.passed("Successfully set cluster name on all nodes")
            return True
        else:
            mylog.error("Failed to set cluster name on all nodes")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-n", "--node_ips", action="list", dest="node_ips", default=None, help="the management IPs of the nodes")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--cluster_name", type="string", dest="cluster_name", default=None, help="the new cluster for the node")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ips, options.cluster_name, options.username, options.password, options.debug):
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

