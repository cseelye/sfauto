#!/usr/bin/python

"""
This action will get the IPMI IP address of a node

When run as a script, the following options/env variables apply:
    --node_ip           Node IP address
    SFNODE_IPS

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS

    --csv               Display minimal output that is suitable for piping into other programs

    --bash              Display minimal output that is formatted for a bash array/for loop
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class GetNodeIpmiIpAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"node_ip" : libsf.IsValidIpv4Address},
            args)

    def Get(self, node_ip, csv=False, bash=False, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Get the IPMI IP address of the node
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

        mylog.info("Determining IPMI IP address for " + node_ip)
        try:
            ipmi_ip = libsf.GetIpmiIp(node_ip, ssh_user, ssh_pass)
        except libsf.SfError as e:
            mylog.error("Failed to get IP: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        self.SetSharedValue(SharedValues.ipmiIP, ipmi_ip)
        self.SetSharedValue(node_ip + "-ipmiIP", ipmi_ip)
        return ipmi_ip

    def Execute(self, node_ip, csv=False, bash=False, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Show the IPMI IP address of the node
        """
        del self
        ipmi_ip = Get(**locals())
        if ipmi_ip is False:
            return False

        if csv or bash:
            sys.stdout.write(ipmi_ip)
            sys.stdout.flush()
        else:
            mylog.info("Node " + node_ip + " IPMI IP address is " + ipmi_ip)
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
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ip, options.csv, options.bash, options.ssh_user, options.ssh_pass, options.debug):
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

