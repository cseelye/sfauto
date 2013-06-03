#!/usr/bin/python

"""
This action will reboot a node

When run as a script, the following options/env variables apply:
    --node_ip           Node IP address

    --ssh_user          The node SSH username
    SFSSH_USER env var

    --ssh_pass          The node SSH password
    SFSSH_PASS
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.libsfnode as libsfnode
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class RebootNodeAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_REBOOT = "BEFORE_REBOOT"
        AFTER_REBOOT = "AFTER_REBOOT"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"node_ip" : libsf.IsValidIpv4Address,
                            },
            args)

    def Execute(self, node_ip=None, waitForUp=True, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Reboot a node
        """
        if not node_ip:
            node_ip = self.GetSharedValue("nodeIP")
        if not node_ip:
            node_ip = self.GetNextSharedValue("activeNodeList")
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        self._RaiseEvent(self.Events.BEFORE_REBOOT)

        node = libsfnode.SFNode(node_ip, ssh_user, ssh_pass)

        mylog.info("Rebooting " + node_ip)
        try:
            node.Reboot(waitForUp)
            pass
        except libsf.SfError as e:
            mylog.error("Failed to reboot node: " + str(e))
            self.RaiseFailureEvent(message=str(e), nodeIP=node_ip, exception=e)
            return False

        mylog.passed(node_ip + " rebooted successfully")
        self._RaiseEvent(self.Events.AFTER_REBOOT)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--node_ip", type="string", dest="node_ip", default=None, help="the IP address of the node")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--nowait", action="store_false", default=True, dest="wait", help="do not wait for the node to come back up")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ip, options.wait, options.ssh_user, options.ssh_pass, options.debug):
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

