#!/usr/bin/python

"""
This action will enable the root account on a set of nodes

When run as a script, the following options/env variables apply:
    --node_ips          The list of node management IP addresses
    SFNODE_IPS

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import otp.libotp as libotp
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class EnableNodeRootAction(ActionBase):
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

    def Execute(self, node_ips=None, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Enable root on a list of nodes
        """
        if not node_ips:
            node_ips = sfdefaults.node_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        allgood = True
        for node_ip in node_ips:
            mylog.info("Enabling root on " + node_ip)
            try:
                libotp.ExecSshCommand(node_ip, ssh_user, ssh_pass, "echo diamondsRforever | sudo -S sed -e 's/PermitRootLogin no/PermitRootLogin yes/' -e's/AllowUsers sfadmin/AllowUsers */' -i /etc/ssh/sshd_config")
                libotp.ExecSshCommand(node_ip, ssh_user, ssh_pass, "echo diamondsRforever | sudo -S restart ssh")
                libotp.ExecSshCommand(node_ip, ssh_user, ssh_pass, "echo diamondsRforever | sudo -S ln -s /etc/alternatives/solidfire-otp /root/.otpw")
            except libsf.SfError as e:
                mylog.error(node_ip + ": " + str(e))
                self.RaiseFailureEvent(message=str(e), nodeIP=node_ip, exception=e)
                allgood = False

        if allgood:
            mylog.passed("Successfully enabled root on all nodes")
            return True
        else:
            mylog.error("Failed to enable root on all nodes")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-n", "--node_ips", action="list", dest="node_ips", default=None, help="the IP addresses of the nodes")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default="sfadmin", help="the SSH username for the nodes")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ips, options.ssh_user, options.ssh_pass, options.debug):
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
