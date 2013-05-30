#!/usr/bin/python

"""
This action will set the hostname of a node

When run as a script, the following options/env variables apply:
    --node_ip           The node management IP

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --hostname          New hostname for the node
"""

import sys
from optparse import OptionParser
import logging
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import lib.libsfnode as libsfnode

class SetNodeHostnameAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_SET_HOSTNAME = "BEFORE_SET_HOSTNAME"
        AFTER_SET_HOSTNAME = "AFTER_SET_HOSTNAME"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"nodeIP" : libsf.IsValidIpv4Address,
                            "hostname": None},
            args)

    def Execute(self, nodeIP, hostname, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Set the hostname on a node
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        self._RaiseEvent(self.Events.BEFORE_SET_HOSTNAME, hostname=hostname, nodeIP=nodeIP)
        mylog.info("Setting hostname to '" + hostname + "' on node " + str(nodeIP))

        node = libsfnode.SFNode(nodeIP, clusterUsername=username, clusterPassword=password)
        try:
            node.SetHostname(hostname)
        except libsf.SfError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), nodeIP=nodeIP, hostname=hostname, exception=e)
            return False

        mylog.passed("Successfully set hostname on " + nodeIP)
        self._RaiseEvent(self.Events.AFTER_SET_HOSTNAME, nodeIP=nodeIP, hostname=hostname)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-n", "--node_ip", type="string", dest="node_ip", default=None, help="the management IP of the node")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--hostname", type="string", dest="hostname", default=None, help="the new hostname for the node")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ip, options.hostname, options.username, options.password, options.debug):
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

