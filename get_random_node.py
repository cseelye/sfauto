#!/usr/bin/python

"""
This action will select a random node from the cluster

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --ensemble          Only select from ensemble nodes

    --nomaster          Do not select the cluster master

    --csv               Display minimal output that is suitable for piping into other programs

    --bash              Display minimal output that is formatted for a bash array/for loop
"""

import sys
from optparse import OptionParser
from random import randint
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class GetRandomNodeAction(ActionBase):
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

    def Get(self, mvip, ensemble=False, nomaster=False, csv=False, bash=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Select a random node
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

        master_id = None
        try:
            result = libsf.CallApiMethod(mvip, username, password, 'GetClusterMasterNodeID', {})
        except libsf.SfError as e:
            mylog.error("Failed to get cluster master: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        master_id = result["nodeID"]
        #mylog.debug("Cluster master is nodeID " + str(master_id))

        node_list = []
        if ensemble:
            # Find the nodes in the cluster
            node_ref = dict()
            try:
                result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
            except libsf.SfError as e:
                mylog.error("Failed to get node list: " + e.message)
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            for node in result["nodes"]:
                if node["nodeID"] == master_id:
                    mylog.debug("Cluster master is " + node["mip"])
                    if nomaster:
                        continue
                node_ref[node["cip"]] = node["mip"]

            # Get the ensemble list
            try:
                result = libsf.CallApiMethod(mvip, username, password, 'GetClusterInfo', {})
            except libsf.SfError as e:
                mylog.error("Failed to get cluster info: " + e.message)
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            for node_cip in result["clusterInfo"]["ensemble"]:
                if node_cip in node_ref:
                    node_list.append(node_ref[node_cip])
            node_count = len(node_list)
            node_list.sort()
            mylog.debug("Found " + str(node_count) + " eligible nodes in cluster " + mvip + ": " + ",".join(node_list))
        else:
            # Find the nodes in the cluster
            try:
                result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
            except libsf.SfError as e:
                mylog.error("Failed to get node list: " + e.message)
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            for node in result["nodes"]:
                if node["nodeID"] == master_id:
                    mylog.debug("Cluster master is " + node["mip"])
                    if nomaster:
                        continue
                node_list.append(node["mip"])
            node_count = len(node_list)
            node_list.sort()
            mylog.debug("Found " + str(node_count) + " eligible nodes in cluster " + mvip + ": " + ",".join(node_list))

        index = randint(0, len(node_list)-1)
        node_ip = node_list[index]

        self.SetSharedValue(SharedValues.nodeIP, node_ip)
        return node_ip

    def Execute(self, mvip, ensemble=False, nomaster=False, csv=False, bash=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Show a random node
        """
        del self
        node_ip = Get(**locals())
        if node_ip is False:
            return False

        if bash or csv:
            sys.stdout.write(str(node_ip) + "\n")
            sys.stdout.flush()
        else:
            mylog.info("Selected " + str(node_ip))
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
    parser.add_option("--ensemble", action="store_true", dest="ensemble", default=False, help="only select from ensemble nodes")
    parser.add_option("--nomaster", action="store_true", dest="nomaster", default=False, help="do not select the cluster master")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.ensemble, options.nomaster, options.csv, options.bash, options.username, options.password, options.debug):
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

