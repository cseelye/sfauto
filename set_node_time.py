#!/usr/bin/python

"""
This action will set the time on a node

When run as a script, the following options/env variables apply:
    --node_ip           The node management IP

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --new_time          The new time to set - string in a format the "date" command recognizes
"""

import sys
from optparse import OptionParser
import logging
import time
import lib.libsf as libsf
from lib.libsf import mylog
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import lib.libsfnode as libsfnode
import lib.sfdefaults as sfdefaults


class SetNodeTimeAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_SET_TIME = "BEFORE_SET_TIME"
        AFTER_SET_TIME = "AFTER_SET_TIME"
        SET_TIME_FAILED = "SET_TIME_FAILED"
        TIME_SET_FUTURE = "TIME_SET_FUTURE"
        TIME_SET_PAST = "TIME_SET_PAST"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"nodeIPs" : libsf.IsValidIpv4AddressList,
                            "newTime" : None},
            args)

    def Execute(self, nodeIPs, newTime, sshUser=sfdefaults.ssh_user, sshPass=sfdefaults.ssh_pass, debug=False):
        """
        Set the time on a node
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        allgood = True
        for node_ip in nodeIPs:
            self._RaiseEvent(self.Events.BEFORE_SET_TIME, nodeIP=node_ip, newTime=newTime)
            mylog.info("Setting time to '" + newTime + "' on node " + str(node_ip))

            node = libsfnode.SFNode(node_ip, sshUser, sshPass)
            try:
                node_time = node.SetTime(newTime)
            except libsf.SfError as e:
                mylog.error(str(e))
                self._RaiseEvent(self.Events.SET_TIME_FAILED, nodeIP=node_ip, newTime=newTime, exception=e)
                allgood = False
                continue

            mylog.passed("Successfully set time on " + node_ip)
            self._RaiseEvent(self.Events.AFTER_SET_TIME, nodeIP=node_ip, newTime=newTime)

        if time.time() > node_time:
            self._RaiseEvent(self.Events.TIME_SET_PAST, nodeIP=node_ip, newTime=newTime)
        else:
            self._RaiseEvent(self.Events.TIME_SET_FUTURE, nodeIP=node_ip, newTime=newTime)

        if allgood:
            mylog.passed("Successfully set time on all nodes")
            return True
        else:
            mylog.error("Failed to set time on all nodes")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--node_ip", type="string", dest="node_ip", default=None, help="the IP address of the node")
    parser.add_option("--new_time", type="string", dest="new_time", default=None, help="the time to set on the node - string in a format 'date' will accept")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ip, options.new_time, options.ssh_user, options.ssh_pass, options.debug):
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
