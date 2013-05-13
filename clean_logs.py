#!/usr/bin/python

"""
This action will clear the logs on a set of nodes

When run as a script, the following options/env variables apply:
    --node_ips          The list of node management IP addresses
    SFNODE_IPS

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS

    --save_bundle       Save a support bundle before clearing the logs
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase

class CleanLogsAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"node_ips" : libsf.IsValidIpv4AddressList},
            args)

    def Execute(self, node_ips=None, save_bundle=False, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Clear the logs on a list of nodes
        """
        if not node_ips:
            node_ips = sfdefaults.node_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        allgood = True
        for node_ip in node_ips:
            mylog.info(node_ip + ": Connecting to node")
            try:
                ssh = libsf.ConnectSsh(node_ip, ssh_user, ssh_pass)

                if save_bundle:
                    mylog.info(node_ip + ": Saving support bundle")
                    libsf.ExecSshCommand(ssh, "/sf/scripts/sf_make_support_bundle `date +%Y-%m-%d-%H-%M-%S` 2>&1 >/dev/null")

                mylog.info(node_ip + ": Clearing SF logs")

                # get rid of old logs
                libsf.ExecSshCommand(ssh, "rm -f /var/log/sf-*.gz")

                # empty current logs
                libsf.ExecSshCommand(ssh, "for f in `ls -1 /var/log/sf-*`; do echo \"Log cleared on `date +%Y-%m-%d-%H-%M-%S`\" > f; done")

                ssh.close()
            except libsf.SfError as e:
                mylog.error("Failed to clear logs on " + node_ip + ": " + str(e))
                super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, nodeIP=node_ip, exception=e)
                allgood = False
                continue

        if allgood:
            mylog.passed("Successfully cleared logs on all nodes")
            return True
        else:
            mylog.error("Could not clear logs on all nodes")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-n", "--node_ips", action="list", dest="node_ips", default=None, help="the IP addresses of the nodes")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes")
    parser.add_option("--save_bundle", action="store_true", dest="save_bundle", default=False, help="save a support bundle before clearing logs")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ips, options.save_bundle, options.ssh_user, options.ssh_pass, options.debug):
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

