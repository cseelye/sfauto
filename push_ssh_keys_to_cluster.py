#!/usr/bin/python

"""
This action will push the local SSH RSA key to all of the nodes in a cluster, to enable password-less SSH

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
import platform
import os
import lib.libsf as libsf
from lib.libsf import mylog
import otp.libotp as libotp
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class PushSshKeysToClusterAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_PUSH = "BEFORE_PUSH"
        AFTER_PUSH = "AFTER_PUSH"
        ALL_PUSHED = "ALL_PUSHED"
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
        Push SSH keys to nodes
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        if "win" in platform.system().lower():
            mylog.error("Sorry, this action does not work on Windows")
            return False

        # Get the list of node IPs from the cluster
        node_ips = []
        mylog.info("Getting a list of nodes in cluster " + mvip)
        try:
            result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for node in result["nodes"]:
            node_ips.append(node["mip"])

        try:
            result = libsf.CallApiMethod(mvip, username, password, 'ListPendingNodes', {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for node in result["pendingNodes"]:
            node_ips.append(node["mip"])


        # Get local hostname
        local_hostname = platform.node()

        # Look for or create local RSA id
        home = os.path.expanduser("~")
        key_text = ""
        if "win" in platform.system().lower():
            key_path = home + "\\ssh\\id_rsa.pub"
        else:
            key_path = home + "/.ssh/id_rsa.pub"

        if not os.path.exists(key_path):
            if "win" in platform.system().lower():
                mylog.error("Please create an RSA key first ")
                return False
            else:
                mylog.info("Creating SSH key for " + local_hostname)
                libsf.RunCommand("ssh-keygen -q -f ~/.ssh/id_rsa -N \"\"")

        # Read the key
        with open(key_path) as f:
            key_text = f.read()
        key_text = key_text.rstrip()

        # Send the key over to each node
        allgood = True
        for node_ip in node_ips:
            self._RaiseEvent(self.Events.BEFORE_PUSH, nodeIP=node_ip)
            try:
                # See if the key is already on the node
                mylog.info("Checking authorized_keys on " + node_ip)
                stdout, stderr, return_code = libotp.ExecSshCommand(node_ip, ssh_user, ssh_pass, "grep '" + key_text + "' ~/.ssh/authorized_keys")
                if return_code == 0:
                    mylog.info("Key is already on node " + node_ip)
                else:
                    # Add the key if it is not present
                    mylog.info("Adding key to " + node_ip)
                    libotp.ExecSshCommand(node_ip, ssh_user, ssh_pass, "mkdir ~/.ssh; chmod 700 ~/.ssh")
                    libotp.ExecSshCommand(node_ip, ssh_user, ssh_pass, "echo \"" + key_text + "\" >> ~/.ssh/authorized_keys; chmod 600 ~/.ssh/authorized_keys")
            except libsf.SfError as e:
                mylog.error("Failed to push key to node " + node_ip + " - " + str(e))
                self.RaiseFailureEvent(message=str(e), nodeIP=node_ip, exception=e)
                allgood = False
                continue
            self._RaiseEvent(self.Events.AFTER_PUSH, nodeIP=node_ip)

        self._RaiseEvent(self.Events.ALL_PUSHED)
        if allgood:
            mylog.passed("Successfully pushed SSH keys to all nodes")
            return True
        else:
            mylog.error("Could not push SSH keys to all nodes")
            return False

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
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
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
