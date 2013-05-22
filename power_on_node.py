#!/usr/bin/python

"""
This action will power on a node

When run as a script, the following options/env variables apply:
    --node_ip           Node IP address

    --ssh_user          The node SSH username
    SFSSH_USER env var

    --ssh_pass          The node SSH password
    SFSSH_PASS

    --ipmi_ip           The IPMI IP address of the node

    --ipmi_user         The username for IPMI
    SFIPMI_USER env var

    --ipmi_user         The password for IPMI
    SFIPMI_USER env var
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

class PowerOnNodeAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_POWERON = "BEFORE_POWERON"
        AFTER_POWERON = "AFTER_POWERON"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"node_ip" : libsf.IsValidIpv4Address,
                            "ipmi_ip" : libsf.IsValidIpv4Address,
                            },
            args)

    def Execute(self, node_ip, ipmi_ip, ipmi_user=sfdefaults.ipmi_user, ipmi_pass=sfdefaults.ipmi_pass, debug=False):
        """
        Power on a node
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        node = libsfnode.SFNode(node_ip, ipmiIP=ipmi_ip, ipmiUsername=ipmi_user, ipmiPassword=ipmi_pass)

        mylog.info("Powering on node " + node_ip)
        self._RaiseEvent(self.Events.BEFORE_POWERON)
        try:
            node.PowerOn()
        except libsf.SfError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), nodeIP=node_ip, exception=e)
            return False

        self._RaiseEvent(self.Events.AFTER_POWERON)
        mylog.passed(node_ip + " powered on successfully")
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--node_ip", type="string", dest="node_ip", default=None, help="the IP address of the node")
    parser.add_option("--ipmi_ip", type="string", dest="ipmi_ip", default=None, help="the IPMI IP address of the node")
    parser.add_option("--ipmi_user", type="string", dest="ipmi_user", default=sfdefaults.ipmi_user, help="the IPMI username for the nodes")
    parser.add_option("--ipmi_pass", type="string", dest="ipmi_pass", default=sfdefaults.ipmi_pass, help="the IPMI password for the nodes")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ip, options.ipmi_ip, options.ipmi_user, options.ipmi_pass, options.debug):
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

