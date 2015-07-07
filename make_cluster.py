"""
This script will take in an N number of nodes and turn them into a cluster

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --cluster_name      The name of the cluster to create

    --node_ips          A list of nodes to make the cluster with

    --node_count        The number of nodes 

    --ssh_user          The ssh username for the nodes

    --ssh_pass          The ssh password for the nodes

"""


import sys
import time
from optparse import OptionParser
import logging
import platform
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import lib.libclient as libclient
from lib.action_base import ActionBase

import cluster_sfnodereset
import create_cluster
import set_node_cluster
import check_cluster_health

class MakeClusterAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_ALL = "BEFORE_ALL"
        AFTER_ALL = "AFTER_ALL"
        BEFORE_CLIENT_LOGOUT = "BEFORE_CLIENT_LOGOUT"
        AFTER_CLIENT_LOGOUT = "AFTER_CLIENT_LOGOUT"
        BEFORE_CLIENT_CLEAN = "BEFORE_CLIENT_CLEAN"
        AFTER_CLIENT_CLEAN = "AFTER_CLIENT_CLEAN"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, svip=sfdefaults.svip, nodeIPs=None, nodeCount=None, clusterName=None, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):

        #sfnodereset
        mylog.step("Resetting the nodes")
        if cluster_sfnodereset.Execute(node_ips=nodeIPs, ssh_user=ssh_user, ssh_pass=ssh_pass) == False:
            mylog.error("Could not reset all the nodes:" + ", ".join(nodeIps))
            return False
        #wait
        time.sleep(30)

        #set the cluster name
        mylog.step("Setting the cluster name to " + clusterName)
        if set_node_cluster.Execute(nodeIPs=nodeIPs, clusterName=clusterName, username=username, password=password) == False:
            mylog.error("Could not set the cluster name")
            return False

        #make the cluster
        mylog.step("Creating the cluster")
        if create_cluster.Execute(node_ip=nodeIPs[0], mvip=mvip, svip=svip, username=username, password=password, add_drives=True, node_count=nodeCount) == False:
            mylog.error("Could not create the cluster")
            return False

        #wait
        time.sleep(30) 

        #check health
        mylog.step("Making sure the cluster is healthy")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password, ssh_user=ssh_user, ssh_pass=ssh_pass) == False:
            mylog.error("The cluster is not healthy")
            return False
        
        mylog.passed("The cluster has been created")
        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="The IP address of the cluster")
    parser.add_option("-s", "--svip", type="string", dest="svip", default=sfdefaults.svip, help="The SVIP address of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster  [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster  [%default]")
    parser.add_option("--node_ips", action="list", dest="node_ips", default=None, help="the IP addresses of the nodes")
    parser.add_option("--cluster_name", type="string", dest="cluster_name", default=None, help="The name of the cluster")
    parser.add_option("--node_count", type="int", dest="node_count", default=3, help="How many nodes to be in the cluster, min = 3")
    parser.add_option("--ssh_user", type="string", dest="ssh_username", default=sfdefaults.ssh_username, help="the ssh account for the node  [%default]")
    parser.add_option("--ssh_pass", type="string", dest="ssh_password", default=sfdefaults.ssh_password, help="the admin password for the node  [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.svip, options.node_ips, options.node_count, options.cluster_name, options.ssh_username, options.ssh_password, options.debug):
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

