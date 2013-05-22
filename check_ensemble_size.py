#!/usr/bin/python

"""
This action will check the size of the ensemble

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
import logging
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CheckEnsembleSizeAction(ActionBase):
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
                            },
            args)

    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Check the size of the ensemble
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Validate args
        if not mvip:
            raise libsf.SfArgumentError("Please specify an MVIP")
        if not libsf.IsValidIpv4Address(mvip):
            raise libsf.SfArgumentError("'" + str(mvip) + "' does not appear to be a valid MVIP")

        # Find the nodes in the cluster
        node_list = dict()
        try:
            result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for node in result["nodes"]:
            node_list[node["nodeID"]] = node["cip"]
        node_count = len(node_list.keys())
        mylog.info("Found " + str(node_count) + " nodes in cluster")

        # Get the ensemble list
        try:
            result = libsf.CallApiMethod(mvip, username, password, 'GetClusterInfo', {})
        except libsf.SfError as e:
            mylog.error("Failed to get cluster info: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        mylog.info("Ensemble list: " + str(result["clusterInfo"]["ensemble"]))
        ensemble_count = len(result["clusterInfo"]["ensemble"])

        # Make sure we have the correct number of ensemble members
        if node_count < 3: # Less then 3 node, ensemble of 1
            if ensemble_count == 1:
                mylog.passed("Found " + str(ensemble_count) + " ensemble nodes")
                return True
            else:
                mylog.error("Found " + str(ensemble_count) + " ensemble nodes but expected 1")
                return False
        elif node_count < 5: # 3-4 nodes, ensemble of 3
            if ensemble_count == 3:
                mylog.passed("Found " + str(ensemble_count) + " ensemble nodes")
                return True
            else:
                mylog.error("Found " + str(ensemble_count) + " ensemble nodes but expected 3")
                return False
        else: #  5 or more nodes, ensemble of 5
            if ensemble_count == 5:
                mylog.passed("Found " + str(ensemble_count) + " ensemble nodes")
                return True
            else:
                mylog.error("Found " + str(ensemble_count) + " ensemble nodes but expected 5")
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
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
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
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

