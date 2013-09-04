#!/usr/bin/python

"""
This action will check the health of the cluster

Healthy is currently defined as no faults, no cores, no xUnknownBlockID

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

    --since             When to check health from (unix timestamp). Any problems from before this will be ignored

    --fault_whitelist   Ignore these faults if they are present
    SFFAULT_WHITELIST env var

    --ignore_faults     Do ot check for cluster faults

    --ignore_cores      Do not check for core files
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
import lib.libsfnode as libsfnode
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CheckClusterHealthAction(ActionBase):
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

    def Execute(self, mvip=sfdefaults.mvip, ignoreCoreFiles=False, ignoreFaults=False, fault_whitelist=None, since=0, username=sfdefaults.username, password=sfdefaults.password, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Check the health of the cluster
        """
        if fault_whitelist == None:
            fault_whitelist = sfdefaults.fault_whitelist
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster = libsfcluster.SFCluster(mvip, username, password)

        # Get the list of nodes in the cluster
        try:
            node_ips = cluster.ListActiveNodeIPs()
        except libsf.SfError as e:
            mylog.error("Failed to get list of nodes: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        healthy = True

        # Check for core files
        mylog.info("Checking for core files on nodes")
        found_cores = []
        for node_ip in node_ips:
            #mylog.info("Checking for core files on " + node_ip)
            node = libsfnode.SFNode(node_ip, ssh_user, ssh_pass)
            try:
                core_list = node.GetCoreFileList(since)
            except libsf.SfError as e:
                mylog.error("Failed to check for core files: " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            if len(core_list) > 0:
                found_cores.append(node_ip)
                if not ignoreCoreFiles:
                    healthy = False
                    mylog.error("Found " + str(len(core_list)) + " core files on " + node_ip + ": " + ",".join(core_list))
        if ignoreCoreFiles and found_cores:
            mylog.warning("Core files present on " + ",".join(found_cores))

        # Check for xUnknownBlockID
        mylog.info("Checking for errors in cluster event log")
        try:
            if cluster.CheckForEvent("xUnknownBlockID", since):
                healthy = False
                mylog.error("Found xUnknownBlockId in the event log")
        except libsf.SfError as e:
            mylog.error("Failed to check events: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Check current cluster faults
        mylog.info("Checking for unresolved cluster faults")

        fault_whitelist = set(fault_whitelist)
        # if len(fault_whitelist) > 0:
            # mylog.info("If these faults are present, they will be ignored: " + ", ".join(fault_whitelist))

        try:
            current_faults = cluster.GetCurrentFaultSet()
        except libsf.SfError as e:
            mylog.error("Failed to get list of faults: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        if current_faults.intersection(fault_whitelist):
            mylog.info("Current whitelisted cluster faults found: " + ", ".join(str(s) for s in current_faults.intersection(fault_whitelist)))

        if current_faults.difference(fault_whitelist):
            found_faults = True
            if ignoreFaults:
                mylog.warning("Current cluster faults found: " + ", ".join(str(s) for s in current_faults.difference(fault_whitelist)))
            else:
                healthy = False
                mylog.error("Current cluster faults found: " + ", ".join(str(s) for s in current_faults.difference(fault_whitelist)))


        if healthy:
            mylog.passed("Cluster is healthy")
            return True
        else:
            mylog.error("Cluster is not healthy")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes")
    parser.add_option("--since", type="int", dest="since", default=0, help="timestamp of when to check health from")
    parser.add_option("--fault_whitelist", action="list", dest="fault_whitelist", default=None, help="ignore these faults and do not wait for them to clear")
    parser.add_option("--ignore_cores", action="store_true", dest="ignoreCoreFiles", default=False, help="ignore core files on nodes")
    parser.add_option("--ignore_faults", action="store_true", dest="ignoreFaults", default=False, help="ignore cluster faults")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.ignoreCoreFiles, options.ignoreFaults, options.fault_whitelist, options.since, options.username, options.password, options.ssh_user, options.ssh_pass, options.debug):
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
    sys.exit(0)
