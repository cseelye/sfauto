#!/usr/bin/python

"""
This action will set an artificial network delay between two clusters

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the first cluster
    SFMVIP env var

    --mvip2             The managementVIP of the second cluster

    --user              The cluster admin username for the first cluster
    SFUSER env var

    --pass              The cluster admin password for the first cluster
    SFPASS env var

    --user2             The cluster admin username for the second cluster

    --pass2             The cluster admin password for the second cluster

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS env var

    --delay             The delay (ms) to set
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class SetClusterDelayAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "mvip2" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "username2" : None,
                            "password2" : None,
                            },
            args)

    def Execute(self, mvip=sfdefaults.mvip, mvip2=None, delay=0, vary=0, username=sfdefaults.username, password=sfdefaults.password, username2=sfdefaults.username, password2=sfdefaults.password, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Set a network delay between two clusters
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster1 = libsfcluster.SFCluster(mvip, username, password)
        cluster2 = libsfcluster.SFCluster(mvip2, username2, password2)

        # On each node in cluster 1,
        #   On each Bond interface,
        #      Delete any existing tc rules
        #      Create the root queue
        #      Create the dealy queue
        #      Add each mIP of cluster2
        #      Add each cIP of cluster2
        #      Add the mVIP of cluster2
        #      Add the sVIP of cluster2

        mylog.info("Getting a list of nodes in cluster " + mvip)
        cluster1_nodelist = cluster1.ListActiveNodes()

        mylog.info("Getting a list of nodes in cluster " + mvip2)
        cluster2_nodelist = cluster2.ListActiveNodes()
        cluster2_svip = cluster2.GetClusterInfo()["svip"]

        for node in cluster1_nodelist:
            ssh = libsf.ConnectSsh(node["mip"], ssh_user, ssh_pass)
            # If we are on the carbon pairing/slice/etc branch, switch to carbon-dev
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "cp /etc/apt/sources.list /tmp/sources.list")
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "sed -i 's/carbon\S*-updates/carbon-updates/' /etc/apt/sources.list")
            # Make sure the package is installed
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "sfapt update")
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "sfapt -y install netemul")
            # Restore the original sources.list
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "cp /tmp/sources.list /etc/apt/sources.list")

            # Delete the existing filters, queues, etc on Bond1G
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "tc qdisc del root dev Bond1G")
            # Delete the existing filters, queues, etc on Bond10G
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "tc qdisc del root dev Bond10G")

            for dev_name in ["Bond1G", "Bond10G"]:
                mylog.info("Configuring " + dev_name + " on " + node["mip"])
                # Create the root queue
                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "tc qdisc add dev " + dev_name + " root handle 1: prio")
                # Create the netem delay queue
                command = "tc qdisc add dev " + dev_name + " parent 1:3 handle 30: netem delay " + str(delay) + "ms"
                if vary:
                    command += " " + str(vary) + "ms"
                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
                # Add the mIP and cIP of cluster2's nodes
                for remote_node in cluster2_nodelist:
                    stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "tc filter add dev " + dev_name + " parent 1:0 prio 3 u32 match ip dst " + remote_node["mip"] + " flowid 1:3")
                    stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "tc filter add dev " + dev_name + " parent 1:0 prio 3 u32 match ip dst " + remote_node["cip"] + " flowid 1:3")
                # Add cluster2's mVIP and sVIP
                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "tc filter add dev " + dev_name + " parent 1:0 prio 3 u32 match ip dst " + cluster2.mvip + " flowid 1:3")
                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "tc filter add dev " + dev_name + " parent 1:0 prio 3 u32 match ip dst " + cluster2_svip+ " flowid 1:3")

        mylog.passed("Successfully configured delay of " + str(delay) + " between cluster " + mvip + " and cluster " + mvip2)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line options
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the first cluster")
    parser.add_option("--mvip2", type="string", dest="mvip2", default=None, help="the management VIP for the second cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the first cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the first cluster [%default]")
    parser.add_option("--user2", type="string", dest="username2", default=sfdefaults.username, help="the username for the second cluster [%default]")
    parser.add_option("--pass2", type="string", dest="password2", default=sfdefaults.password, help="the password for the second cluster [%default]")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--delay", type="int", dest="delay", default=0, help="the delay to set, in ms")
    parser.add_option("--vary", type="int", dest="vary", default=0, help="the random variation to apply to the delay, in ms")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()
    if extra_args and len(extra_args) > 0:
        mylog.error("Unknown arguments: " + ",".join(extra_args))
        sys.exit(1)

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.mvip2, options.delay, options.vary, options.username, options.password, options.username2, options.password2, options.ssh_user, options.ssh_pass, options.debug):
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
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

