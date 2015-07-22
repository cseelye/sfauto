#!/usr/bin/env python2.7

"""
This action will create a new VLAN interface on a client

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients
    SFCLIENT_IPS env var

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --vlan_base         The base interface to create the VLAN interface on

    --vlan_tag          The tag of the VLAN

    --vlan_ip           The IP address to use on the VLAN interface

    --vlan_netmask      The subnet mask to use on the VLAN interface
"""

import sys
from optparse import OptionParser
import re
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CreateClientVlanAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"client_ip" : libsf.IsValidIpv4Address,
                            "vlan_tag" : libsf.IsInteger,
                            "vlan_ip" : libsf.IsValidIpv4Address,
                            "vlan_netmask" : libsf.IsValidIpv4Address},
            args)

    def Execute(self, vlan_base, vlan_tag, vlan_ip, vlan_netmask, client_ip=None, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_user, debug=False):
        """
        Create a VLAN interface on a client
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Creating VLAN interface {}.{} on {}".format(vlan_base, vlan_tag, client_ip))
        try:
            ssh = libsf.ConnectSsh(client_ip, client_user, client_pass)
            libsf.ExecSshCommand(ssh, "vconfig add {} {}".format(vlan_base, vlan_tag))
            libsf.ExecSshCommand(ssh, "ifconfig {}.{} {} netmask {} up".format(vlan_base, vlan_tag, vlan_ip, vlan_netmask))
        except libsf.SfError as e:
            mylog.error(str(e))
            return False

        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--client_ip", type="string", dest="client_ip", default=None, help="the IP addresses of the client")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
    parser.add_option("--vlan_base", type="string", dest="vlan_base", default=None, help="the base interface to create the VLAN interface on")
    parser.add_option("--vlan_tag", type="int", dest="vlan_tag", default=None, help="the VLAN tag")
    parser.add_option("--vlan_ip", type="string", dest="vlan_ip", default=None, help="the IP to use on the VLAN interface")
    parser.add_option("--vlan_netmask", type="string", dest="vlan_netmask", default=None, help="the subnet mask to use on the VLAN interface")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vlan_base, options.vlan_tag, options.vlan_ip, options.vlan_netmask, options.client_ip, options.client_user, options.client_pass, options.debug):
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
