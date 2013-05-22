#!/usr/bin/python

"""
This action will check the health of the ensemble

Healthy is currently defined as the correct ensemble size and zk responding on all nodes

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS
"""

import sys
from optparse import OptionParser
import re
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CheckEnsembleHealthAction(ActionBase):
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

    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Check the health of the ensemble
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Check the status of the MVIP
        mylog.info("Checking the MVIP")
        if not libsf.Ping(mvip):
            mylog.error("Cannot ping " + mvip)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Find the nodes in the cluster
        node_list = dict()
        try:
            result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for node in result["nodes"]:
            node_list[node["cip"]] = node["mip"]
        node_count = len(node_list.keys())
        mylog.info("Found " + str(node_count) + " nodes in the cluster (" + ",".join(sorted(node_list.values())) + ")")

        # Get the ensemble list
        try:
            result = libsf.CallApiMethod(mvip, username, password, 'GetClusterInfo', {})
        except libsf.SfError as e:
            mylog.error("Failed to get cluster info: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        ensemble_nodes = sorted(result["clusterInfo"]["ensemble"])
        ensemble_count = len(ensemble_nodes)
        mylog.info("Found " + str(ensemble_count) + " nodes in the ensemble (" + ",".join(ensemble_nodes) + ")")

        # Make sure we have the correct number of ensemble members
        if node_count < 3 and ensemble_count != 1: # Less then 3 node, ensemble of 1
            mylog.error("Found " + str(ensemble_count) + " ensemble nodes but expected 1")
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        elif node_count < 5 and ensemble_count != 3: # 3-4 nodes, ensemble of 3
            mylog.error("Found " + str(ensemble_count) + " ensemble nodes but expected 3")
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        elif node_count >= 5 and ensemble_count != 5: #  5 or more nodes, ensemble of 5
            mylog.error("Found " + str(ensemble_count) + " ensemble nodes but expected 5")
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Make sure we can connect to and query all of the ensemble servers
        for node_ip in sorted(node_list.values()):
            mylog.info("Connecting to " + node_ip)
            try:
                ssh = libsf.ConnectSsh(node_ip, ssh_user, ssh_pass)
                for cip in ensemble_nodes:
                    mylog.info("  Checking ZK server at " + cip)
                    stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "zkCli.sh -server " + cip + " get /ensemble; echo $?")
                    lines = stdout.readlines()
                    if (int(lines.pop().strip()) != 0):
                        mylog.error("Could not query zk server on " + cip)
                        for line in stderr.readlines():
                            mylog.error(line.rstrip())
                        return False
            except libsf.SfError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

        mylog.info("Getting the ensemble report")
        ensemble_report = libsf.HttpRequest("https://" + mvip + "/reports/ensemble", username, password)
        if not ensemble_report:
            mylog.error("Failed to get ensemble report")
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        if "error" in ensemble_report:
            m = re.search(r"<pre>(x\S+)", ensemble_report)
            if m:
                if "xRecvTimeout" in m.group(1):
                    mylog.warning("xRecvTimeout but ensemble looks otherwise healthy")
                    return True
                mylog.error("Ensemble error detected: " + m.group(1))
            else:
                mylog.error("Ensemble error detected")
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.passed("Ensemble is healthy")
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster [%default]")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.ssh_user, options.ssh_pass, options.debug):
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

